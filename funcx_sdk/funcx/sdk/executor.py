import time
import threading
import os
import uuid
import sys
from concurrent.futures import Future
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


class FuncXExecutorFuture(Future):
    def __init__(self, function_future_map, poller_thread):
        super().__init__()

        self._function_future_map = function_future_map
        self._poller_thread = poller_thread
        self._result_checked = False
    
    def result(self, timeout=None):
        self._result_checked = True
        self._open_poller_thread()

        # We will expect the user to attempt a future.result() call to all the futures
        # before we close the poller thread
        try:
            res = super().result(timeout)
        except Exception:
            self._close_poller_thread()
            raise

        self._close_poller_thread()
        return res

    def _open_poller_thread(self):
        # open the poller thread if it was closed but this result is still pending
        if not super().done():
            self._poller_thread.start()

    def _close_poller_thread(self):
        # close the poller thread if all futures in the future map are either done
        # or the user has already attempted to call future.result() on them
        for task_uuid in self._function_future_map:
            fut = self._function_future_map[task_uuid]
            done_or_result_checked = fut.done() or fut._result_checked
            if not done_or_result_checked:
                return

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
        self.task_group_id = self.funcx_client.session_task_group_id  # we need to associate all batch launches with this id

        self.poller_thread = ExecutorPollerThread(self.funcx_client, self._function_future_map, self.results_ws_uri, self.task_group_id)
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
        self._function_future_map[task_uuid] = FuncXExecutorFuture(self._function_future_map, self.poller_thread)

        res = self._function_future_map[task_uuid]

        self.poller_thread.start()
        return res

    def shutdown(self):
        if self.poller_thread:
            self.poller_thread.shutdown()
        logger.debug(f"Executor:{self.label} shutting down")


class ExecutorPollerThread():
    """ An executor
    """

    def __init__(self, funcx_client, _function_future_map, results_ws_uri, task_group_id):
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

        self.eventloop = None
        self.ws_handler = None
        self.ws_initialized = threading.Event()

    def start(self):
        """ Start the result polling thread
        """

        if self.ws_handler:
            return

        # Currently we need to put the batch id's we launch into this queue
        # to tell the web_socket_poller to listen on them. Later we'll associate

        self.ws_initialized.clear()

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
            self.ws_initialized.set()
            await self.ws_handler.handle_incoming(self._function_future_map)
        except Exception:
            # TODO: handle when ws initialization fails, possibly by setting an exception
            # on all of the outstanding futures in the future map
            raise

    def shutdown(self):
        if not self.ws_handler:
            return

        self.ws_initialized.wait()

        ws = self.ws_handler.ws
        if ws:
            ws_close_future = asyncio.run_coroutine_threadsafe(ws.close(), self.eventloop)
            ws_close_future.result()
        
        self.ws_handler = None


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
