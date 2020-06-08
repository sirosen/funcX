import sys
import logging

try:
    from termcolor import colored
except ImportError:
    def colored(x, *args, **kwargs):
        return x

from parsl.app.errors import RemoteExceptionWrapper


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter(colored("[WORKER]    %(message)s", 'red')))
logger.addHandler(ch)


class LocalExecutor(object):

    def __init__(self, task_queue, result_queue, log_level='INFO'):
        self.task_queue = task_queue
        self.result_queue = result_queue

        logger.setLevel(log_level)

        self.start()

    def start(self):

        logger.info('Starting local worker')

        while True:

            logger.debug("Waiting for task")
            data = self.task_queue.get()
            task_id = data['task_id']

            logger.debug("Executing task...")

            try:
                result = self.execute_task(data)
            except Exception as e:
                logger.exception(f"Caught an exception {e}")
                result = {
                    'task_id': task_id,
                    'exception': RemoteExceptionWrapper(*sys.exc_info())
                }
            else:
                logger.debug("Execution completed without exception")
                result = {
                    'task_id': task_id,
                    'result': result
                }

            self.result_queue.put(result)

        logger.warning("Broke out of the loop... dying")

    def execute_task(self, data):
        f = data['function']
        args = data['args']
        kwargs = data['kwargs']

        return f(*args, **kwargs)
