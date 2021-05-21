import asyncio
import json
import logging
from asyncio import AbstractEventLoop, QueueEmpty
import dill
import websockets
import threading
from websockets.exceptions import InvalidHandshake, ConnectionClosedOK

from funcx.sdk.asynchronous.funcx_task import FuncXTask

logger = logging.getLogger("asyncio")


class WebSocketPollingTask:
    """
    """

    def __init__(self, funcx_client,
                 loop: AbstractEventLoop,
                 init_task_group_id: str = None,
                 results_ws_uri: str = 'ws://localhost:6000',
                 dead_event=None,
                 lock=None,
                 auto_start: bool = True):
        """
        Parameters
        ==========

        funcx_client : client object
            Instance of FuncXClient to be used by the executor

        loop : event loop
            The asnycio event loop that the WebSocket client will run on

        init_task_group_id : str
            Optional task_group_id UUID string that the WebSocket client
            can immediately poll for after initialization

        results_ws_uri : str
            Web sockets URI for the results

        auto_start : Bool
            Set this to start the WebSocket client immediately.
            Otherwise init_ws must be called.
            Default: True
        """
        self.funcx_client = funcx_client
        self.loop = loop
        self.init_task_group_id = init_task_group_id
        self.results_ws_uri = results_ws_uri
        self.auto_start = auto_start
        self.running_task_group_ids = set()
        # add the initial task group id, as this will be sent to
        # the WebSocket server immediately
        self.running_task_group_ids.add(self.init_task_group_id)
        self.task_group_ids_queue = asyncio.Queue()
        self.pending_tasks = {}
        self.dead_event = dead_event if dead_event else threading.Event()
        self.lock = lock

        self.raw_data_queue = asyncio.Queue()

        self.ws = None
        self.closed = False

        if auto_start:
            self.loop.create_task(self.init_ws())

    async def init_ws(self, start_message_handlers=True):
        headers = [self.get_auth_header()]
        try:
            self.ws = await websockets.client.connect(self.results_ws_uri, extra_headers=headers)
        # initial Globus authentication happens during the HTTP portion of the handshake,
        # so an invalid handshake means that the user was not authenticated
        except InvalidHandshake:
            raise Exception('Failed to authenticate user. Please ensure that you are logged in.')

        if self.init_task_group_id:
            await self.ws.send(self.init_task_group_id)

        if start_message_handlers:
            self.loop.create_task(self.send_outgoing(self.task_group_ids_queue))
            self.loop.create_task(self.handle_incoming(self.pending_tasks))

    async def send_outgoing(self, queue: asyncio.Queue):
        while True:
            task_group_id = await queue.get()
            await self.ws.send(task_group_id)

    async def handle_incoming(self, pending_futures, auto_close=False):
        while True:
            # logger.warning("[WS_POLLING_T] Waiting for data")
            try:
                raw_data = await self.ws.recv()
            # we should get this exception after ws.close is called
            except ConnectionClosedOK:
                return
            except Exception as e:
                logger.exception(e)
                return

            self.raw_data_queue.put_nowait(raw_data)
            # this needs to happen in an async task because there is blocking to
            # acquire the lock. This is safe because the lock will only allow one
            # async task to attempt to close the WebSocket, and only if no other
            # messages should be received on this WebSocket
            self.loop.create_task(self.handle_message(pending_futures, auto_close))

    async def handle_message(self, pending_futures, auto_close):
        # LOCK HERE - when we start checking the future map
        if self.lock:
            self.lock.acquire()
            logger.debug('ACQUIRE LOCK - WS POLLING')
        
        raw_data_processed_count = 0
        while True:
            # if there is no raw data available, all that will happen is that the
            # lock will simply be acquired then released again
            try:
                raw_data = self.raw_data_queue.get_nowait()
            except Exception:
                break
            
            raw_data_processed_count += 1

            data = json.loads(raw_data)
            task_id = data['task_id']

            # logger.warning(f"[WS_POLLING_T] Received task: {task_id}")
            if task_id in pending_futures:

                future = pending_futures[task_id]
                del pending_futures[task_id]

                try:
                    if data['result']:
                        future.set_result(self.funcx_client.fx_serializer.deserialize(data['result']))
                    elif data['exception']:
                        r_exception = self.funcx_client.fx_serializer.deserialize(data['exception'])
                        future.set_exception(dill.loads(r_exception.e_value))
                    else:
                        future.set_exception(Exception(data['reason']))
                except Exception as e:
                    print("Caught exception : ", e)

                # logger.warning("WS_POLLING_TASK] pending futures: {len(pending_futures)}")
                if auto_close and len(pending_futures) == 0:
                    self.dead_event.set()
                    # logger.warning("[WS_POLLING_TASK] setting dead event")
                    await self.ws.close()
                    self.ws = None
                    self.closed = True
            else:
                print("[MISSING FUTURE]")

        # UNLOCK HERE - after the WebSocket is closed if it needs closing
        if self.lock:
            logger.debug('RELEASE LOCK - WS POLLING')
            self.lock.release()

    def put_task_group_id(self, task_group_id):
        # prevent the task_group_id from being sent to the WebSocket server
        # multiple times
        if task_group_id not in self.running_task_group_ids:
            self.running_task_group_ids.add(task_group_id)
            self.task_group_ids_queue.put_nowait(task_group_id)

    def add_task(self, task: FuncXTask):
        """
        Add a funcX task
        :param task: FuncXTask
            Task to be added
        """
        self.pending_tasks[task.task_id] = task

    def get_auth_header(self):
        """
        Gets an Authorization header to be sent during the WebSocket handshake. Based on
        header setting in the Globus SDK: https://github.com/globus/globus-sdk-python/blob/main/globus_sdk/base.py

        Returns
        -------
        Key-value tuple of the Authorization header
        (key, value)
        """
        headers = dict()
        self.funcx_client.authorizer.set_authorization_header(headers)
        header_name = 'Authorization'
        return (header_name, headers[header_name])
