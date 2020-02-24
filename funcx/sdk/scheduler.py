import time
import random
import logging
from collections import defaultdict
from threading import Thread

from funcx.sdk.client import FuncXClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
logger.addHandler(ch)


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
                 *args, **kwargs):
        self._fxc = fxc or FuncXClient(*args, **kwargs)
        # Special Dill serialization so that wrapped methods work correctly
        self._fxc.fx_serializer.use_custom('03\n', 'code')

        # List of all FuncX endpoints we can execute on
        self._endpoints = list(set(endpoints)) or []

        # Average times for each function on each endpoint
        # TODO: this info is too unrealistic to have
        self._runtimes = defaultdict(lambda: defaultdict(float))
        self._num_executions = defaultdict(lambda: defaultdict(int))

        # Track all pending tasks
        self._pending_tasks = {}
        self._results = {}

        # Scheduling strategy
        if strategy not in self.STRATEGIES:
            raise ValueError("strategy must be one of {}"
                             .format(self.STRATEGIES))
        self.strategy = strategy

        # Start a thread to wait for results and record runtimes
        self._watchdog_sleep_time = 0.1  # in seconds
        self._watchdog_thread = Thread(target=self._wait_for_results)
        self._watchdog_thread.start()

    def add_endpoint(self, endpoint_id):
        self._endpoints.add(endpoint_id)

    def remove_endpoiint(self, endpoint_id):
        self._endpoints.discard(endpoint_id)

    def register_function(self, function, *args, **kwargs):
        wrapped_function = timer(function)
        return self._fxc.register_function(wrapped_function, *args, **kwargs)

    def run(self, *args, function_id, asynchronous=False, **kwargs):
        endpoint_id = self._choose_best_endpoint(*args,
                                                 function_id=function_id,
                                                 **kwargs)
        info = {
            'time_sent': time.time(),
            'function_id': function_id,
            'endpoint_id': endpoint_id
        }

        task_id = self._fxc.run(*args, function_id=function_id,
                                endpoint_id=endpoint_id,
                                asynchronous=asynchronous, **kwargs)

        logger.debug('Sent function {} to endpoint {} with task_id {}'
                     .format(function_id, endpoint_id, task_id))

        self._pending_tasks[task_id] = info
        return task_id

    def get_result(self, task_id, block=False):
        if task_id not in self._pending_tasks and task_id not in self._results:
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

    def _round_robin(self, *args, function_id, **kwargs):
        if not hasattr(self, '_rr_count'):
            self._rr_count = 0

        endpoint = self._endpoints[self._rr_count]
        self._rr_count = (self._rr_count + 1) % len(self._endpoints)
        return endpoint

    def _fastest(self, *args, function_id, exploration=False, **kwargs):
        if not hasattr(self, '_next_endpoint'):
            self._next_endpoint = defaultdict(int)

        # Try each endpoint once, and then start choosing the best one
        if self._next_endpoint[function_id] < len(self._endpoints):
            endpoint = self._endpoints[self._next_endpoint[function_id]]
            self._next_endpoint[function_id] += 1
            return endpoint
        elif exploration and random.random() < 0.2:
            return random.choice(self._endpoints)
        else:
            endpoint, _ = min(self._runtimes[function_id].items(),
                              key=lambda x: x[1])
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

        logger.info('[WATCHDOG] Thread started')

        while True:
            to_delete = set()
            # Convert to list first because otherwise, the dict may throw an
            # exception that its size has changed during iteration. This can
            # happen when new pending tasks are added to the dict.
            for task_id, info in list(self._pending_tasks.items()):
                try:
                    res = self._fxc.get_result(task_id)
                except Exception as e:
                    if not str(e).startswith("Task pending"):
                        logger.warn('[WATCHDOG] Got unexpected exception:\t{}'
                                    .format(e))
                        raise
                    continue

                logger.debug('[WATCHDOG] Got result for task {} with runtime {}'
                             .format(task_id, res['runtime']))
                self._update_average(task_id, new_time=res['runtime'])
                self._results[task_id] = res['result']
                to_delete.add(task_id)

                # Sleep, to prevent being throttled
                time.sleep(self._watchdog_sleep_time)

            for task_id in to_delete:
                del self._pending_tasks[task_id]

    def _update_average(self, task_id, new_time):
        info = self._pending_tasks[task_id]
        function_id = info['function_id']
        endpoint_id = info['endpoint_id']

        num_times = self._num_executions[function_id][endpoint_id]
        old_avg = self._runtimes[function_id][endpoint_id]
        new_avg = (old_avg * num_times + new_time) / (num_times + 1)
        self._runtimes[function_id][endpoint_id] = new_avg
        self._num_executions[function_id][endpoint_id] += 1


def funcx_sum(items):
    return sum(items)


if __name__ == "__main__":

    endpoints = [
        # add your own funcx endpoints
        # '4b116d3c-1703-4f8f-9f6f-39921e5864df',  # public tutorial endpoint
    ]

    fxc = FuncXClient()
    sched = FuncXScheduler(fxc, endpoints=endpoints,
                           strategy='fastest-with-exploration')
    func_uuid = sched.register_function(funcx_sum, description="Summer")
    payload = [2, 34]

    task_ids = []
    for i in range(10):
        task_id = sched.run(payload, function_id=func_uuid)
        task_ids.append(task_id)
        print('task_id:', task_id)

    for task_id in task_ids:
        res = sched.get_result(task_id, block=True)
        print('Got res:', res)
