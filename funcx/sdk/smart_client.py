import os
import sys
import time
import uuid
import logging
from collections import defaultdict
from threading import Thread
# import multiprocessing as mp
from queue import Queue, Empty


try:
    from termcolor import colored
except ImportError:
    def colored(x, *args, **kwargs):
        return x

from funcx.sdk.client import FuncXClient

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


class FuncXSmartClient(object):
    def __init__(self, fxc=None, log_level='DEBUG', *args, **kwargs):

        self._fxc = fxc or FuncXClient(*args, **kwargs)
        # Special Dill serialization so that wrapped methods work correctly
        self._fxc.fx_serializer.use_custom('03\n', 'code')

        # Track all pending tasks (organized by endpoint) and results
        self._pending = {}
        self._results = {}
        self._completed_tasks = set()

        # Set logging levels
        logger.setLevel(log_level)
        watchdog_logger.setLevel(log_level)
        self.execution_log = []

        self.running = True

        # Start a thread to wait for results and record runtimes
        self._watchdog_sleep_time = 0.01  # in seconds
        self._watchdog_thread = Thread(target=self._wait_for_results)
        self._watchdog_thread.start()

    def register_function(self, function, *args, **kwargs):
        wrapped_function = timer(function)
        func_id = self._fxc.register_function(wrapped_function, *args, **kwargs)
        return func_id

    def run(self, *args, function_id, asynchronous=False, **kwargs):
        endpoint_id = 'UNDECIDED'
        task_id = self._fxc.run(*args, function_id=function_id,
                                endpoint_id=endpoint_id,
                                asynchronous=asynchronous, **kwargs)
        self._add_pending_task(*args, task_id=task_id,
                               function_id=function_id,
                               endpoint_id=endpoint_id, **kwargs)

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
        elif task_id in self._completed_tasks:
            raise Exception("Task result already returned")
        else:
            raise Exception("Task pending")

    def stop(self):
        self.running = False
        self._watchdog_thread.join()

    def _wait_for_results(self):
        '''Watchdog thread function'''

        watchdog_logger.info('Thread started')

        while self.running:
            to_delete = set()

            # Convert to list first because otherwise, the dict may throw an
            # exception that its size has changed during iteration. This can
            # happen when new pending tasks are added to the dict.
            for task_id, info in list(self._pending.items()):

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


##############################################################################
#                           Utility Functions
##############################################################################


def avg(x):
    if isinstance(x, Queue):
        x = x.queue

    return sum(x) / len(x)
