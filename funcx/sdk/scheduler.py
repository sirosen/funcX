import os
import sys
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
    BLACKLIST_ERRORS = [ModuleNotFoundError, MemoryError]
    BACKUP_THRESHOLD = 1.3

    def __init__(self, fxc=None, endpoints=None, strategy='round-robin',
                 use_full_exec_time=True, log_level='INFO',
                 last_n_times=3, max_backups=None,
                 *args, **kwargs):
        self._fxc = fxc or FuncXClient(*args, **kwargs)
        # Special Dill serialization so that wrapped methods work correctly
        self._fxc.fx_serializer.use_custom('03\n', 'code')

        # List of all FuncX endpoints we can execute on
        self._endpoints = list(set(endpoints)) or []
        # Track which endpoints a function can't run on
        self._blacklists = defaultdict(set)

        # Average times for each function on each endpoint
        self._last_n_times = last_n_times
        self._exec_times = defaultdict(lambda: defaultdict(Queue))
        self._runtimes = defaultdict(lambda: defaultdict(Queue))
        self._avg_exec_time = defaultdict(lambda: defaultdict(float))
        self._avg_runtime = defaultdict(lambda: defaultdict(float))
        self._num_executions = defaultdict(lambda: defaultdict(int))
        self.use_full_exec_time = use_full_exec_time

        # Track all pending tasks (organized by endpoint) and results
        self._pending = {}
        self._pending_by_endpoint = defaultdict(Queue)
        self._results = {}
        self._completed_tasks = set()
        self._backups = defaultdict(set)
        self.max_backups = max_backups or len(self._endpoints) - 1
        self._poll_count = 0

        # Scheduling strategy
        if strategy not in self.STRATEGIES:
            raise ValueError("strategy must be one of {}"
                             .format(self.STRATEGIES))
        self.strategy = strategy
        logger.info(f"Scheduler using strategy {strategy}")

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

        # Scheduling strategy
        if strategy not in self.STRATEGIES:
            raise ValueError("strategy must be one of {}"
                             .format(self.STRATEGIES))
        self.strategy = strategy

        # Start a thread to do local execution
        self._functions = {}
        self._local_task_queue = mp.Queue()
        self._local_result_queue = mp.Queue()
        self._local_worker_process = mp.Process(target=LocalExecutor,
                                                args=(self._local_task_queue,
                                                      self._local_result_queue))
        self._local_worker_process.start()

        # Start a thread to wait for results and record runtimes
        self._watchdog_sleep_time = 0.01  # in seconds
        self._watchdog_thread = Thread(target=self._wait_for_results)
        self._watchdog_thread.start()

    def add_endpoint(self, endpoint_id):
        self._endpoints.append(endpoint_id)
        self._all_endpoints_explored = False

    def remove_endpoint(self, endpoint_id):
        self._endpoints.remove(endpoint_id)

    def register_function(self, function, *args, **kwargs):
        wrapped_function = timer(function)
        func_id = self._fxc.register_function(wrapped_function, *args, **kwargs)
        self._functions[func_id] = wrapped_function
        return func_id

    def run(self, *args, function_id, asynchronous=False, backup_of=None,
            exclude=None, **kwargs):

        # TODO: potential race condition since run can now be called from
        # _send_backup_if_needed from watchdog thread concurrently with
        # a user's call to it. should synchronize access to this method.

        endpoint_id = self._choose_best_endpoint(*args, function_id=function_id,
                                                 exclude=exclude, **kwargs)

        if endpoint_id == 'local':
            task_id = self._run_locally(*args, function_id=function_id,
                                        **kwargs)
        else:
            task_id = self._fxc.run(*args, function_id=function_id,
                                    endpoint_id=endpoint_id,
                                    asynchronous=asynchronous, **kwargs)
            self._add_pending_task(*args, task_id=task_id,
                                   function_id=function_id,
                                   endpoint_id=endpoint_id,
                                   backup_of=backup_of, **kwargs)

        logger.debug('Sent function {} to endpoint {} with task_id {}'
                     .format(function_id, endpoint_id, task_id))

        return task_id

    def _send_backup_if_needed(self, task_id):
        info = self._pending[task_id]
        function_id = info['function_id']
        endpoint_id = info['endpoint_id']

        # Don't send backup jobs for backup jobs
        if info['backup_of'] is not None:
            return

        # Don't send backup unless we know expected runtime
        if endpoint_id not in self._avg_exec_time[function_id]:
            return

        # Don't send backup unless task is overdue
        elapsed_time = time.time() - info['time_sent']
        expected_time = self._avg_exec_time[function_id][endpoint_id]
        if elapsed_time < self.BACKUP_THRESHOLD * expected_time:
            return

        # Don't send more than max number of backups
        if len(self._backups[task_id]) >= self.max_backups:
            return

        # Don't send two copies of task to the same endpoint
        exclude = [info['endpoint_id']]
        for backup_id in self._backups[task_id]:
            backup_info = self._pending[backup_id]
            exclude.append(backup_info['endpoint_id'])

        logger.debug('Sending backup task for task_id {}'.format(task_id))
        backup_id = self.run(*info['args'], function_id=function_id,
                             backup_of=task_id, exclude=exclude,
                             **info['kwargs'])

        self._backups[task_id].add(backup_id)

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
        elif task_id in self._completed_tasks:
            raise Exception("Task result already returned")
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

    def _fastest(self, *args, function_id, exploration=False, exclude=None,
                 **kwargs):
        if not hasattr(self, '_next_endpoint'):
            self._next_endpoint = defaultdict(int)
        if not hasattr(self, '_all_endpoints_explored'):
            self._all_endpoints_explored = False

        # Tracked runtimes, if any
        if self.use_full_exec_time:
            times = list(self._avg_exec_time[function_id].items())
        else:
            times = list(self._avg_runtime[function_id].items())

        # Filter out blacklisted endpoints
        exclude = set(exclude or []) | self._blacklists[function_id]

        if len(exclude) == len(self._endpoints):
            raise RuntimeError('All endpoints for function {} blacklisted'
                               ' or already attempted!'.format(function_id))

        times = [(e, t) for (e, t) in times if e not in exclude]

        # Tracked runtimes, if any
        if self.use_full_exec_time:
            times = list(self._exec_times[function_id].items())
        else:
            times = list(self._runtimes[function_id].items())

        # Try each endpoint once, and then start choosing the best one
        if not self._all_endpoints_explored or len(times) == 0:
            endpoint = self._endpoints[self._next_endpoint[function_id]]
            self._next_endpoint[function_id] += 1
            if self._next_endpoint[function_id] == len(self._endpoints):
                self._all_endpoints_explored = True
            self._next_endpoint[function_id] %= len(self._endpoints)
        elif exploration:
            pairs = [(e, 1 / t) for e, t in times]
            _, max_throughput = max(pairs, key=lambda x: x[1])
            pairs = [(e, tp) for (e, tp) in pairs if tp / max_throughput > 0.5]
            endpoints, throughputs = zip(*pairs)
            weights = normalize(throughputs)
            endpoint = random.choices(endpoints, weights=weights)[0]
        else:
            endpoint, _ = min(times, key=lambda x: x[1])

        return endpoint

    def _choose_best_endpoint(self, *args, exclude=None, **kwargs):
        if self.strategy == 'round-robin':
            return self._round_robin(*args, **kwargs)
        elif self.strategy == 'random':
            return random.choice(self._endpoints)
        elif self.strategy == 'fastest':
            return self._fastest(self, *args, exclude=exclude, **kwargs)
        elif self.strategy == 'fastest-with-exploration':
            return self._fastest(self, *args, exclude=exclude,
                                 exploration=True, **kwargs)
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

                # Sleep, to prevent being throttled
                time.sleep(self._watchdog_sleep_time)

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
                    self._poll_count += 1
                    res = self._fxc.get_result(task_id)
                    self._record_result(task_id, res)
                except Exception as e:
                    if str(e).startswith("Task pending"):
                        # Put popped task back into front of queue
                        tasks.queue.appendleft((task_id, info))
                        self._send_backup_if_needed(task_id)
                        continue
                    else:
                        self._blacklist_if_needed(task_id)
                        watchdog_logger.error('Exception on task {}:\t{}'
                                              .format(task_id, e))
                        self._results[task_id] = f'Exception: {e}'

                to_delete.add(task_id)

            # Stop tracking all tasks which have now returned
            for task_id in to_delete:
                del self._pending[task_id]

    def _ready_to_poll(self, task_id, p=0.5):
        info = self._pending[task_id]
        function_id = info['function_id']
        endpoint_id = info['endpoint_id']

        # If no prior information, always poll
        if endpoint_id not in self._avg_exec_time[function_id]:
            return True

        elapsed_time = time.time() - info['time_sent']
        expected_time = self._avg_exec_time[function_id][endpoint_id]

        return expected_time < 0.5 or elapsed_time > p * expected_time

    def _add_pending_task(self, *args, task_id, function_id, endpoint_id,
                          backup_of=None, **kwargs):
        info = {
            'time_sent': time.time(),
            'function_id': function_id,
            'endpoint_id': endpoint_id,
            'args': args,
            'kwargs': kwargs,
            'backup_of': backup_of
        }

        self._pending[task_id] = info
        self._pending_by_endpoint[endpoint_id].put((task_id, info))

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

        # Store runtime and exec time
        self._update_runtimes(task_id, result['runtime'], exec_time)

        # Get main task id in case this task is a backup task
        main_task_id = info['backup_of'] or task_id

        # Only store a copy of a result once per main task
        if main_task_id not in self._completed_tasks:
            self._results[main_task_id] = result['result']
            self._completed_tasks.add(main_task_id)
        else:
            watchdog_logger.debug('Ignoring result for {} since another result '
                                  'for its original task was already recorded'
                                  .format(task_id))

        info['exec_time'] = exec_time
        self.execution_log.append(info)

    def _update_runtimes(self, task_id, new_runtime, new_exec_time):
        info = self._pending[task_id]
        func = info['function_id']
        end = info['endpoint_id']

        while len(self._runtimes[func][end].queue) > self._last_n_times:
            self._runtimes[func][end].get()
        self._runtimes[func][end].put(new_runtime)
        self._avg_runtime[func][end] = avg(self._runtimes[func][end])

        while len(self._exec_times[func][end].queue) > self._last_n_times:
            self._exec_times[func][end].get()
        self._exec_times[func][end].put(new_exec_time)
        self._avg_exec_time[func][end] = avg(self._exec_times[func][end])

        self._num_executions[func][end] += 1

    def _run_locally(self, *args, function_id, backup_of=None, **kwargs):

        task_id = str(uuid.uuid4())

        task_id = str(uuid.uuid4())

        data = {
            'function': self._functions[function_id],
            'args': args,
            'kwargs': kwargs,
            'task_id': task_id
        }

        # Must add to pending tasks before putting on queue, to prevent
        # race condition when checking for results
        self._add_pending_task(*args, task_id=task_id, function_id=function_id,
                               endpoint_id='local', backup_of=backup_of,
                               **kwargs)
        self._local_task_queue.put(data)

        return task_id

    def _blacklist_if_needed(self, task_id):
        info = self._pending[task_id]
        exc_type, _, _ = sys.exc_info()
        if exc_type in self.BLACKLIST_ERRORS:
            watchdog_logger.warn('Blacklisting endpoint {} for function {} due'
                                 ' to exception'.format(info['endpoint_id'],
                                                        info['function_id']))
            self._blacklists[info['function_id']].add(info['endpoint_id'])


##############################################################################
#                           Utility Functions
##############################################################################

def avg(x):
    if isinstance(x, Queue):
        x = x.queue

    return sum(x) / len(x)


def normalize(xs):
    total = sum(xs)
    return [x / total for x in xs]
