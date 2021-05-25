import time
import threading
import os
import uuid
import sys
from concurrent.futures import Future, TimeoutError
import concurrent
import logging
import asyncio
import websockets
import json
import dill
from websockets.exceptions import InvalidHandshake
import multiprocessing as mp
import atexit

from funcx.sdk.asynchronous.ws_polling_task import WebSocketPollingTask

logger = logging.getLogger(__name__)


class AtomicCounter():

    def __init__(self):
        self._value = 0
        self.lock = threading.Lock()
        # usage of these events is simple: you set is_live when a thread like the WebSocket
        # poller thread is guaranteed to be alive, and you set is_dead when the thread
        # is guaranteed to be dead
        # The essential element is that is_live MUST be set after every 0-to-1 increment,
        # and is_dead MUST be set after every 1-to-0 decrement
        self.is_live = threading.Event()
        self.is_live.clear()
        self.is_dead = threading.Event()
        self.is_dead.set()

    def increment(self):
        with self.lock:
            if self._value == 0:
                # this will only get held up if the thread just started the process of closing
                # (we are expecting this event to come because either the is_dead event is
                # set from initialization above or is_dead will be set due to a recent decrement)
                # we would like to avoid having many threads open so that:
                # 1. we do not accidentally lose a thread and have hanging threads
                # 2. the is_live and is_dead events have a clear meaning that only refer to 1 thread at a time

                # Technically, multiple threads could be running at once without issues. However,
                # this optimization to reduce locking has the drawbacks listed above.
                self.is_dead.wait()
                self.is_dead.clear()
            self._value += 1
            return self._value

    def decrement(self):
        with self.lock:
            self._value -= 1
            if self._value == 0:
                # this will only get held up if the thread just started the process of opening
                # we need to ensure that it has opened properly before we can close it properly
                self.is_live.wait()
                self.is_live.clear()
            return self._value

    def value(self):
        with self.lock:
            return self._value

    def __repr__(self):
        return f"AtomicCounter value:{self._value}, {self.is_live.is_set()}, {self.is_dead.is_set()}"        


class FuncXExecutorFuture(Future):
    def __init__(self, function_future_map, poller_thread, task_counter):
        super().__init__()

        self._function_future_map = function_future_map
        self._poller_thread = poller_thread
        self._task_counter = task_counter
        self._result_checked = False
    
    def result(self, timeout=None):
        if self._result_checked:
            if super().done():
                return super().result(timeout)
            else:
                v = self._task_counter.increment()
                if v == 1:
                    self._poller_thread.start()

        self._result_checked = True

        # We will expect the user to attempt a future.result() call to all the futures
        # before we close the poller thread
        try:
            res = super().result(timeout)
        except TimeoutError:
            self._close_poller_thread()
            raise
        except Exception:
            # we should still close the poller thread in the case of any exception
            # because it means that an exception was set to the future and it is complete
            self._close_poller_thread()
            raise

        self._close_poller_thread()
        return res

    # TODO: we will need to override the future.exception() method as well

    def _close_poller_thread(self):
        # close the poller thread if the user has attempted to call future.done() on
        # all futures
        v = self._task_counter.decrement()
        if v == 0:
            self._poller_thread.shutdown()


class FuncXExecutor(concurrent.futures.Executor):
    """ An executor
    """

    def __init__(self, funcx_client,
                 results_ws_uri: str = 'ws://localhost:6000',
                 label: str = 'FuncXExecutor'):
        """
        Parameters
        ==========

        funcx_client : client object
            Instance of FuncXClient to be used by the executor

        results_ws_uri : str
            Web sockets URI for the results

        label : str
            Optional string label to name the executor.
            Default: 'FuncXExecutor'
        """

        self.funcx_client = funcx_client
        self.results_ws_uri = results_ws_uri
        self.label = label
        self._tasks = {}
        self._function_registry = {}
        self._function_future_map = {}
        self.task_counter = AtomicCounter()
        self.task_group_id = self.funcx_client.session_task_group_id  # we need to associate all batch launches with this id

        self.poller_thread = ExecutorPollerThread(self.funcx_client, self._function_future_map, self.results_ws_uri, self.task_group_id, self.task_counter)
        atexit.register(self.shutdown)

    def submit(self, function, *args, endpoint_id=None, container_uuid=None, **kwargs):
        """Initiate an invocation

        Parameters
        ----------
        function : Function/Callable
            Function / Callable to execute

        *args : Any
            Args as specified by the function signature

        endpoint_id : uuid str
            Endpoint UUID string. Required

        **kwargs : Any
            Arbitrary kwargs

        Returns
        -------
        Future : concurrent.futures.Future
            A future object
        """

        v = self.task_counter.increment()
        if v == 1:
            self.poller_thread.start()

        if function not in self._function_registry:
            # Please note that this is a partial implementation, not all function registration
            # options are fleshed out here.
            logger.debug("Function:{function} is not registered. Registering")
            function_uuid = self.funcx_client.register_function(function,
                                                                function_name=function.__name__,
                                                                container_uuid=container_uuid)
            self._function_registry[function] = function_uuid
            logger.debug(f"Function registered with id:{function_uuid}")
        assert endpoint_id is not None, "endpoint_id key-word argument must be set"

        batch = self.funcx_client.create_batch(task_group_id=self.task_group_id)
        batch.add(*args,
                  endpoint_id=endpoint_id,
                  function_id=self._function_registry[function],
                  **kwargs)
        r = self.funcx_client.batch_run(batch)
        logger.debug(f"Batch submitted to task_group: {self.task_group_id}")

        task_uuid = r[0]
        logger.debug(f'Waiting on results for task ID: {task_uuid}')
        # There's a potential for a race-condition here where the result reaches
        # the poller before the future is added to the future_map
        self._function_future_map[task_uuid] = FuncXExecutorFuture(self._function_future_map, self.poller_thread, self.task_counter)

        res = self._function_future_map[task_uuid]
        return res

    def shutdown(self):
        if self.poller_thread:
            self.poller_thread.shutdown()
        logger.debug(f"Executor:{self.label} shutting down")


class ExecutorPollerThread():
    """ An executor
    """

    def __init__(self, funcx_client, _function_future_map, results_ws_uri, task_group_id, task_counter):
        """
        Parameters
        ==========

        funcx_client : client object
            Instance of FuncXClient to be used by the executor

        results_ws_uri : str
            Web sockets URI for the results
        """

        self.funcx_client = funcx_client
        self.results_ws_uri = results_ws_uri
        self._function_future_map = _function_future_map
        self.task_group_id = task_group_id
        self.task_counter = task_counter

        self.eventloop = None
        self.ws_handler = None

    def start(self):
        """ Start the result polling thread
        """
        eventloop = asyncio.new_event_loop()
        self.eventloop = eventloop
        self.ws_handler = WebSocketPollingTask(self.funcx_client, eventloop, self.task_group_id,
                                               self.results_ws_uri, auto_start=False)
        self.thread = threading.Thread(target=self.event_loop_thread,
                                       args=(eventloop, ))
        self.thread.start()

        logger.debug("Started web_socket_poller thread")

    def event_loop_thread(self, eventloop):
        asyncio.set_event_loop(eventloop)
        eventloop.run_until_complete(self.web_socket_poller())

    async def web_socket_poller(self):
        try:
            await self.ws_handler.init_ws(start_message_handlers=False)
            self.task_counter.is_live.set()
            await self.ws_handler.handle_incoming(self._function_future_map)
        except Exception:
            # TODO: handle when ws initialization fails, possibly by setting an exception
            # on all of the outstanding futures in the future map
            raise

    def shutdown(self):
        # under normal, locked shutdown conditions, ws_handler and ws_handler.ws are
        # guaranteed to be set because shutdown can only be called after task_counter.is_live
        # is set.
        if not self.ws_handler:
            return

        ws = self.ws_handler.ws
        if ws:
            ws_close_future = asyncio.run_coroutine_threadsafe(ws.close(), self.eventloop)
            ws_close_future.result()
        
        self.ws_handler = None

        self.task_counter.is_dead.set()


def double(x):
    return x * 2


if __name__ == '__main__':

    import argparse
    from funcx import FuncXClient
    from funcx import set_stream_logger
    import time

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--service_url", default='http://localhost:5000/v2',
                        help="URL at which the funcx-web-service is hosted")
    parser.add_argument("-e", "--endpoint_id", required=True,
                        help="Target endpoint to send functions to")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    endpoint_id = args.endpoint_id

    # set_stream_logger()
    fx = FuncXExecutor(FuncXClient(funcx_service_address=args.service_url))

    print("In main")
    endpoint_id = args.endpoint_id
    for i in range(10):
        future = fx.submit(double, 5, endpoint_id=endpoint_id)
        print("Got future back : ", future)

    # for i in range(5):
    #     time.sleep(0.2)
    #     # Non-blocking check whether future is done
    #     print("Is the future done? :", future.done())

    # print("Blocking for result")
    # x = future.result()     # <--- This is a blocking call
    # print("Result : ", x)

    # fx.shutdown()
