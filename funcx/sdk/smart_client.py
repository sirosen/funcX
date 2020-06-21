import os
import sys
import time
import uuid
import dill
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

from parsl.app.errors import RemoteExceptionWrapper
from funcx.sdk.client import FuncXClient
from funcx.executors import LocalExecutor


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter(colored("[CLIENT]   %(message)s", 'yellow')))
logger.addHandler(ch)

watchdog_logger = logging.getLogger(__name__ + '_watchdog')
watchdog_logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter(colored("[WATCHDOG] %(message)s", 'green')))
watchdog_logger.addHandler(ch)


class timer(object):
    def __init__(self, func):
        self.func = func
        self.__name__ = "timer"

    def __call__(self, *args, **kwargs):
        import sys
        import time

        # Ensure all required files exist on this endpoint
        for end, files in kwargs['_globus_files'].items():
            for f, _ in files:
                assert(f.startswith('~/.globus_funcx/'))
                path = os.path.expanduser(f)
                if not os.path.exists(path):
                    raise FileNotFoundError(path)

        del kwargs['_globus_files']

        # Run function and time execution
        start = time.time()
        res = self.func(*args, **kwargs)
        runtime = time.time() - start

        # Return result with execution times
        return {
            'runtime': runtime,
            'result': res,
            'imports': list(sys.modules.keys()),
        }


class FuncXSmartClient(object):
    def __init__(self, fxc=None, batch_status=True, local=False,
                 last_n=3, log_level='DEBUG', *args, **kwargs):

        self._fxc = fxc or FuncXClient(*args, **kwargs)
        # Special Dill serialization so that wrapped methods work correctly
        self._fxc.fx_serializer.use_custom('03\n', 'code')

        # Track all pending tasks (organized by endpoint) and results
        self._pending = {}
        self._results = {}
        self._completed_tasks = set()
        self._use_batch_status = batch_status

        # Average times for tasks
        self._last_n = last_n
        self._exec_times = defaultdict(lambda: defaultdict(Queue))
        self._avg_exec_time = defaultdict(lambda: defaultdict(float))
        self._num_executions = defaultdict(lambda: defaultdict(int))

        # Set logging levels
        logger.setLevel(log_level)
        watchdog_logger.setLevel(log_level)
        self.execution_log = []

        self.running = True

        # Start a process to do local execution
        self.local = local
        self._next_is_local = True
        if self.local:
            self._functions = {}
            self._local_task_queue = mp.Queue()
            self._local_result_queue = mp.Queue()
            self._local_worker_process = mp.Process(
                target=LocalExecutor, args=(self._local_task_queue,
                                            self._local_result_queue,
                                            log_level))
            self._local_worker_process.start()

        # Start a thread to wait for results and record runtimes
        self._watchdog_sleep_time = 0.1  # in seconds
        self._watchdog_thread = Thread(target=self._wait_for_results)
        self._watchdog_thread.start()

    def register_function(self, function, *args, **kwargs):
        # Extract imports from function body
        source = dill.source.getsource(function)
        imports = []
        for line in source.strip().split('\n'):
            tokens = line.strip().split(' ')
            if len(tokens) > 1 and tokens[0] == 'import':
                imports.append(tokens[1])

        wrapped_function = timer(function)
        func_id = self._fxc.register_function(wrapped_function, *args,
                                              **kwargs, imports=imports)
        if self.local:
            self._functions[func_id] = wrapped_function
        return func_id

    def run(self, *args, function_id, files=None, asynchronous=False, **kwargs):
        batch = self.create_batch()
        batch.add(*args, function_id=function_id, files=files, **kwargs)
        task_id = self.batch_run(batch)[0]
        return task_id

    def run_locally(self, *args, function_id, **kwargs):
        task_id = str(uuid.uuid4())
        data = {
            'function': self._functions[function_id],
            'args': args,
            'kwargs': dict(kwargs),
            'task_id': task_id
        }
        # Must add to pending tasks before putting on queue, to prevent
        # race condition when checking for results
        self._add_pending_task(*args, task_id=task_id,
                               function_id=function_id,
                               endpoint_id='local', **kwargs)
        self._local_task_queue.put(data)

        return task_id

    def _should_run_locally(self, *args, function_id, **kwargs):
        if not self.local:
            return False

        elif len(self._avg_exec_time[function_id]) < 2:
            ret = self._next_is_local
            self._next_is_local = not self._next_is_local
            return ret

        else:
            endpoint, _ = min(self._avg_exec_time[function_id].items(),
                              key=lambda x: x[1])
            return endpoint == 'local'

    def create_batch(self):
        return self._fxc.create_batch()

    def batch_run(self, batch):
        task_ids = []
        remote_batch = self.create_batch()

        # Select tasks which should be run locally
        for task in batch.tasks:
            if self._should_run_locally(*task['args'],
                                        function_id=task['function'],
                                        **task['kwargs']):
                task_id = self.run_locally(*task['args'],
                                           function_id=task['function'],
                                           **task['kwargs'])

                logger.debug('Sent function {} to endpoint {} with task_id {}'
                             .format(task['function'], 'local', task_id))
                task_ids.append(task_id)

            else:
                remote_batch.tasks.append(task)

        if len(remote_batch.tasks) == 0:
            return task_ids

        # Run all other tasks remotely
        logger.info('Running batch with {} tasks'
                    .format(len(remote_batch.tasks)))
        pairs = self._fxc.batch_run(remote_batch)
        for (task_id, endpoint), task in zip(pairs, remote_batch.tasks):
            self._add_pending_task(*task['args'], task_id=task_id,
                                   function_id=task['function'],
                                   endpoint_id=endpoint, **task['kwargs'])

            logger.debug('Sent function {} to endpoint {} with task_id {}'
                         .format(task['function'], endpoint, task_id))
            task_ids.append(task_id)

        return task_ids

    def get_result(self, task_id, block=False):
        if task_id not in self._pending and task_id not in self._results:
            raise ValueError('Unknown task id {}'.format(task_id))

        if block:
            while task_id not in self._results:
                continue

        if task_id in self._results:
            res = self._results[task_id]
            del self._results[task_id]
            if isinstance(res, RemoteExceptionWrapper):
                res.reraise()
            else:
                return res
        elif task_id in self._completed_tasks:
            raise Exception("Task result already returned")
        else:
            raise Exception("Task pending")

    def block(self, function_id, endpoint_id):
        path = f'block/{function_id}/{endpoint_id}'

        r = self._fxc.get(path)
        if r.http_status != 200:
            raise Exception(r)
        elif r['status'] != 'Success':
            raise ValueError('Failed with reason: {}'.format(r['reason']))
        else:  # Successfully blocked
            return

    def get_execution_log(self):
        path = f'execution_log'
        r = self._fxc.get(path)
        if r.http_status != 200:
            raise Exception(r)
        else:
            client_log = self.execution_log
            scheduler_log = r['log']
            self.execution_log = []
            return {'client_log': client_log, 'scheduler_log': scheduler_log}

    def stop(self):
        self.running = False
        self._watchdog_thread.join()
        if self.local:
            self._local_worker_process.terminate()
            self._local_worker_process.join()

    def _wait_for_results(self):
        '''Watchdog thread function'''

        watchdog_logger.info('Thread started')

        while self.running:
            to_delete = set()

            # Check if there are any local results ready
            while self.local:
                try:
                    local_output = self._local_result_queue.get_nowait()
                    task_id = local_output['task_id']
                    if 'exception' in local_output:
                        res = local_output['exception']
                    elif 'result' in local_output:
                        res = local_output['result']
                    else:
                        logger.warn(f'Unexpected local worker result: {res}')
                    self._record_result(task_id, res)
                    to_delete.add(task_id)

                except Empty:
                    break

                except Exception as e:
                    logger.error(f'Exception on task {task_id}: {e}')

            remote_tasks = [task_id for (task_id, info) in self._pending.items()
                            if info['endpoint_id'] != 'local']

            # Check if there are any remote results ready
            if self._use_batch_status:  # Query task statuses in a batch request

                # Sleep, to prevent being throttled
                time.sleep(self._watchdog_sleep_time)

                batch_status = self._fxc.get_batch_status(remote_tasks)

                for task_id, status in batch_status.items():

                    if status['pending'] == 'True':
                        continue

                    elif 'result' in status:
                        self._record_result(task_id, status['result'])

                    elif 'exception' in status:
                        e = status['exception']
                        watchdog_logger.error('Exception on task {}:\t{}'
                                              .format(task_id, e))
                        self._results[task_id] = e

                    else:
                        watchdog_logger.error('Unknown status for task {}:{}'
                                              .format(task_id, status))

                    to_delete.add(task_id)

            else:   # Query task status one at a time

                for task_id in remote_tasks:

                    # Sleep, to prevent being throttled
                    time.sleep(self._watchdog_sleep_time)

                    try:
                        res = self._fxc.get_result(task_id)
                        self._record_result(task_id, res)
                    except Exception as e:
                        if str(e).startswith("Task pending"):
                            continue
                        else:
                            watchdog_logger.error('Exception on task {}:\t{}'
                                                  .format(task_id, e))
                            self._results[task_id] = f'Exception: {e}'
                            raise

                    to_delete.add(task_id)

            # Stop tracking all tasks which have now returned
            for task_id in to_delete:
                del self._pending[task_id]

    def _add_pending_task(self, *args, task_id, function_id, endpoint_id,
                          **kwargs):
        info = {
            'time_sent': time.time(),
            'function_id': function_id,
            'endpoint_id': endpoint_id,
            'args': args,
            'kwargs': kwargs,
        }

        self._pending[task_id] = info

    def _record_result(self, task_id, result):
        info = self._pending[task_id]

        time_taken = time.time() - info['time_sent']

        watchdog_logger.debug('Got result for task {} from '
                              'endpoint {} with time {}'
                              .format(task_id, info['endpoint_id'], time_taken))

        self._results[task_id] = result['result']
        self._completed_tasks.add(task_id)

        info['exec_time'] = time_taken
        info['runtime'] = result['runtime']
        self.execution_log.append(info)

        # Update runtimes
        func = info['function_id']
        end = info['endpoint_id']
        if end != 'local':
            end = 'remote'

        while len(self._exec_times[func][end].queue) > self._last_n:
            self._exec_times[func][end].get()
        self._exec_times[func][end].put(time_taken)
        self._avg_exec_time[func][end] = avg(self._exec_times[func][end])
        self._num_executions[func][end] += 1


##############################################################################
#                           Utility Functions
##############################################################################


def avg(x):
    if isinstance(x, Queue):
        x = x.queue

    return sum(x) / len(x)
