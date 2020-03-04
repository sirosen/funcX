import time
import uuid
import random
import logging
from collections import defaultdict
from threading import Thread
import multiprocessing as mp
from queue import Queue, Empty

try:
    from termcolor import colored
except ImportError:
    def colored(x, *args, **kwargs):
        return x

from funcx.sdk.client import FuncXClient
from funcx.executors import LocalExecutor

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter(colored("[SCHEDULER] %(message)s", 'yellow')))
logger.addHandler(ch)

watchdog_logger = logging.getLogger(__name__ + '_watchdog')
watchdog_logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter(colored("[WATCHDOG]  %(message)s", 'green')))
watchdog_logger.addHandler(ch)


class timer(object):
    def __init__(self, func):
        self.func = func
        self.__name__ = "timer"

    def __call__(self, *args, **kwargs):
        import time
        start = time.time()
        res = self.func(*args, **kwargs)
        runtime = time.time() - start
        return {
            'runtime': runtime,
            'result': res
        }


class FuncXScheduler:
    STRATEGIES = ['round-robin', 'random', 'fastest',
                  'fastest-with-exploration']

    def __init__(self, fxc=None, endpoints=None, strategy='round-robin',
                 use_full_exec_time=True, log_level='INFO',
                 *args, **kwargs):
        self._fxc = fxc or FuncXClient(*args, **kwargs)
        # Special Dill serialization so that wrapped methods work correctly
        self._fxc.fx_serializer.use_custom('03\n', 'code')

        # List of all FuncX endpoints we can execute on
        self._endpoints = list(set(endpoints)) or []

        # Average times for each function on each endpoint
        # TODO: this info is too unrealistic to have
        self._exec_times = defaultdict(lambda: defaultdict(float))
        self._runtimes = defaultdict(lambda: defaultdict(float))
        self._num_executions = defaultdict(lambda: defaultdict(int))
        self.use_full_exec_time = use_full_exec_time

        # Track all pending tasks (organized by endpoint) and results
        self._pending = {}
        self._pending_by_endpoint = defaultdict(Queue)
        self._results = {}

        # Scheduling strategy
        if strategy not in self.STRATEGIES:
            raise ValueError("strategy must be one of {}"
                             .format(self.STRATEGIES))
        self.strategy = strategy

        # Set logging levels
        logger.setLevel(log_level)
        watchdog_logger.setLevel(log_level)
        self.execution_log = []

        # Start a thread to do local execution
        self.running = True
        self._functions = {}
        self._local_task_queue = mp.Queue()
        self._local_result_queue = mp.Queue()
        self._local_worker_process = mp.Process(target=LocalExecutor,
                                                args=(self._local_task_queue,
                                                      self._local_result_queue))
        self._local_worker_process.start()

        # Start a thread to wait for results and record runtimes
        self._watchdog_sleep_time = 0.1  # in seconds
        self._watchdog_thread = Thread(target=self._wait_for_results)
        self._watchdog_thread.start()

    def add_endpoint(self, endpoint_id):
        self._endpoints.add(endpoint_id)

    def remove_endpoint(self, endpoint_id):
        self._endpoints.discard(endpoint_id)

    def register_function(self, function, *args, **kwargs):
        wrapped_function = timer(function)
        func_id = self._fxc.register_function(wrapped_function, *args, **kwargs)
        self._functions[func_id] = wrapped_function
        return func_id

    def run(self, *args, function_id, asynchronous=False, **kwargs):
        endpoint_id = self._choose_best_endpoint(*args,
                                                 function_id=function_id,
                                                 **kwargs)

        if endpoint_id == 'local':
            task_id = self._run_locally(*args, function_id=function_id,
                                        **kwargs)
        else:
            task_id = self._fxc.run(*args, function_id=function_id,
                                    endpoint_id=endpoint_id,
                                    asynchronous=asynchronous, **kwargs)
            self._add_pending_task(task_id, function_id, endpoint_id)

        logger.debug('Sent function {} to endpoint {} with task_id {}'
                     .format(function_id, endpoint_id, task_id))

        return task_id

    def get_result(self, task_id, block=False):
        if task_id not in self._pending and task_id not in self._results:
            raise ValueError('Unknown task id {}'.format(task_id))

        if block:
            while task_id not in self._results:
                continue

        if task_id in self._results:
            res = self._results[task_id]
            del self._results[task_id]
            return res
        else:
            raise Exception("Task pending")

    def stop(self):
        self.running = False
        self._local_worker_process.terminate()
        self._watchdog_thread.join()

    def _round_robin(self, *args, function_id, **kwargs):
        if not hasattr(self, '_rr_count'):
            self._rr_count = 0

        endpoint = self._endpoints[self._rr_count]
        self._rr_count = (self._rr_count + 1) % len(self._endpoints)
        return endpoint

    def _fastest(self, *args, function_id, exploration=False, **kwargs):
        if not hasattr(self, '_next_endpoint'):
            self._next_endpoint = defaultdict(int)

        # Tracked runtimes, if any
        if self.use_full_exec_time:
            times = list(self._exec_times[function_id].items())
        else:
            times = list(self._runtimes[function_id].items())

        # Try each endpoint once, and then start choosing the best one
        if self._next_endpoint[function_id] < len(self._endpoints):
            endpoint = self._endpoints[self._next_endpoint[function_id]]
            self._next_endpoint[function_id] += 1
            return endpoint
        elif exploration and random.random() < 0.2:
            return random.choice(self._endpoints)
        elif len(times) == 0:  # No runtimes recorded yet
            return random.choice(self._endpoints)
        else:
            endpoint, _ = min(times, key=lambda x: x[1])
            return endpoint

    def _choose_best_endpoint(self, *args, **kwargs):
        if self.strategy == 'round-robin':
            return self._round_robin(*args, **kwargs)
        elif self.strategy == 'random':
            return random.choice(self._endpoints)
        elif self.strategy == 'fastest':
            return self._fastest(self, *args, **kwargs)
        elif self.strategy == 'fastest-with-exploration':
            return self._fastest(self, *args, exploration=True, **kwargs)
        else:
            raise NotImplementedError("TODO: implement other stategies")

    def _wait_for_results(self):
        '''Watchdog thread function'''

        watchdog_logger.info('Thread started')

        while self.running:
            to_delete = set()

            # Check if there are any local results ready
            while True:
                try:
                    local_output = self._local_result_queue.get_nowait()
                    res = local_output['result']
                    task_id = local_output['task_id']
                    self._record_result(task_id, res)
                    to_delete.add(task_id)

                except Empty:
                    break

            # Convert to list first because otherwise, the dict may throw an
            # exception that its size has changed during iteration. This can
            # happen when new pending tasks are added to the dict.
            for endpoint_id, tasks in list(self._pending_by_endpoint.items()):

                # Poll only the first pending task for this endpoint
                try:
                    task_id, info = tasks.get_nowait()
                except Empty:  # No pending tasks for this endpoint
                    continue

                # Local tasks will come from local results queue
                if info['endpoint_id'] == 'local':
                    continue

                # Only poll after some fraction of expected runtime has passed
                if not self._ready_to_poll(task_id):
                    # Put popped task back into front of queue
                    tasks.queue.appendleft((task_id, info))
                    continue

                # If remote, ask funcX service for result
                try:
                    res = self._fxc.get_result(task_id)
                except Exception as e:
                    if not str(e).startswith("Task pending"):
                        watchdog_logger.warn('Got unexpected exception:\t{}'
                                             .format(e))
                        raise

                    # Put popped task back into front of queue
                    tasks.queue.appendleft((task_id, info))
                    continue

                self._record_result(task_id, res)
                to_delete.add(task_id)

                # Sleep, to prevent being throttled
                time.sleep(self._watchdog_sleep_time)

            # Stop tracking all tasks which have now returned
            for task_id in to_delete:
                del self._pending[task_id]

    def _ready_to_poll(self, task_id, p=0.3):
        info = self._pending[task_id]
        function_id = info['function_id']
        endpoint_id = info['endpoint_id']

        elapsed_time = time.time() - info['time_sent']
        expected_time = self._exec_times[function_id][endpoint_id]

        return expected_time < 1.0 or elapsed_time > p * expected_time

    def _add_pending_task(self, task_id, function_id, endpoint_id):
        info = {
            'time_sent': time.time(),
            'function_id': function_id,
            'endpoint_id': endpoint_id
        }

        self._pending[task_id] = info
        self._pending_by_endpoint[endpoint_id].put((task_id, info))

    def _record_result(self, task_id, result):
        info = self._pending[task_id]
        exec_time = time.time() - info['time_sent']
        time_taken = exec_time if self.use_full_exec_time else result['runtime']

        watchdog_logger.debug('Got result for task {} from '
                              'endpoint {} with time {}'
                              .format(task_id, info['endpoint_id'], time_taken))
        self._update_average(task_id, result['runtime'], exec_time)
        self._results[task_id] = result['result']

        info['exec_time'] = exec_time
        self.execution_log.append(info)

    def _update_average(self, task_id, new_runtime, new_exec_time):
        info = self._pending[task_id]
        function_id = info['function_id']
        endpoint_id = info['endpoint_id']

        num_times = self._num_executions[function_id][endpoint_id]
        old_avg = self._exec_times[function_id][endpoint_id]
        new_avg = (old_avg * num_times + new_exec_time) / (num_times + 1)
        self._exec_times[function_id][endpoint_id] = new_avg

        old_avg = self._runtimes[function_id][endpoint_id]
        new_avg = (old_avg * num_times + new_runtime) / (num_times + 1)
        self._runtimes[function_id][endpoint_id] = new_avg

        self._num_executions[function_id][endpoint_id] += 1

    def _run_locally(self, *args, function_id, **kwargs):

        task_id = str(uuid.uuid4())

        data = {
            'function': self._functions[function_id],
            'args': args,
            'kwargs': kwargs,
            'task_id': task_id
        }

        # Must add to pending tasks before putting on queue, to prevent
        # race condition when checking for results
        self._add_pending_task(task_id, function_id, 'local')
        self._local_task_queue.put(data)

        return task_id
