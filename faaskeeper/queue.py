import json
import logging
import socket
import time
import urllib.request
from datetime import datetime
from enum import Enum
from queue import Empty, Queue
from threading import Event, Thread
from typing import Optional, Tuple, Union

from faaskeeper.exceptions import (
    ProviderException,
    SessionClosingException,
    TimeoutException,
)
from faaskeeper.node import Node
from faaskeeper.operations import Operation
from faaskeeper.providers.provider import ProviderClient
from faaskeeper.threading import Future


def wait_until(timeout: float, interval: float, condition, *args):
    """A simple hack to wait for an event until a specified length of time passes.

    :param timeout: time to wait for a result [seconds]
    :param interval: sleep time - defines how frequently we check for a result [seconds]
    :param condition: event condition to be evaluated
    :param args: arguments passed to the condition function
    """
    start = time.time()
    while not condition(*args) and time.time() - start < timeout:
        time.sleep(interval)


"""
    Architecture of the receive queue systems on the client.
    We must handle three types of events:
    - direct results of reading from the storage.
    - results of operations returned from serverless workers.
    - watch events delivered.

    The event queue is not necessarily ordered and is used to to transmit results to the
    ordering thread.

    The submission queue is FIFO and is used to transmit requests to the submitter thread.
"""


class EventQueue:
    class EventType(Enum):
        CLOUD_INDIRECT_RESULT = 0
        CLOUD_DIRECT_RESULT = 1
        CLOUD_EXPECTED_RESULT = 2

    """
        The queue is used to handle replies and watch notifications from the service.
        Its second responsibility is ensuring that the results are correctly ordered.

        The queue is served by a single thread processing events.
        In the current implementation, callbacks block the only thread.
    """

    def __init__(self):
        self._queue = Queue()
        self._closing = False

    def add_expected_result(self, request_id: int, request: Operation, future: Future):
        if self._closing:
            raise SessionClosingException()

        self._queue.put((EventQueue.EventType.CLOUD_EXPECTED_RESULT, request_id, request, future))

    def add_direct_result(self, request_id: int, result: Union[Node, Exception], future: Future):
        if self._closing:
            raise SessionClosingException()

        self._queue.put((EventQueue.EventType.CLOUD_DIRECT_RESULT, request_id, result, future))

    def add_indirect_result(self, result: dict):
        if self._closing:
            raise SessionClosingException()

        self._queue.put((EventQueue.EventType.CLOUD_INDIRECT_RESULT, result))

    def get(self) -> Optional[Tuple]:
        try:
            return self._queue.get(block=True, timeout=0.5)
        except Empty:
            return None

    def close(self):
        self._closing = True


class WorkQueue:
    def __init__(self):
        self._queue = Queue()
        self._closing = False
        self._request_count = 0

    def add_request(self, op: Operation, fut: Future):
        if self._closing:
            raise SessionClosingException()

        self._queue.put((self._request_count, op, fut))
        self._request_count += 1

    def get(self) -> Optional[Tuple[int, Operation, Future]]:
        try:
            return self._queue.get(block=True, timeout=0.5)
        except Empty:
            return None

    def close(self):
        self._closing = True

    def wait_close(self, timeout: float = -1):
        if timeout > 0:
            wait_until(timeout, 0.1, self._queue.empty)
            if not self._queue.empty():
                raise TimeoutException(timeout)


class ResponseListener(Thread):
    """The thread receives replies and watch notifications from the service.
    After calling `run`, the thread runs in the background until `stop` is called.

    :param event_queue: reference to the event queue processing replies
    :param port: port to be used for listening for replies, defalts to -1
    """

    @property
    def address(self):
        return self._public_addr

    @property
    def port(self):
        return self._port

    def __init__(self, event_queue: EventQueue, port: int = -1):

        super().__init__(daemon=True)
        self._event_queue = event_queue
        self._work_event = Event()
        self._work_event.set()

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self._socket.bind(("", port if port != -1 else 0))

        req = urllib.request.urlopen("https://checkip.amazonaws.com")
        self._public_addr = req.read().decode().strip()
        self._port = self._socket.getsockname()[1]
        self._log = logging.getLogger("ResponseListener")

        self.start()

    def run(self):

        self._socket.settimeout(0.5)
        self._socket.listen(1)
        self._log.info(f"Begin listening on {self._public_addr}:{self._port}")
        while self._work_event.is_set():

            try:
                conn, addr = self._socket.accept()
            except socket.timeout:
                pass
            except Exception as e:
                raise e
            else:
                self._log.info(f"Connected with {addr}")
                data = json.loads(conn.recv(1024).decode())
                self._log.info(f"Received message: {data}")
                self._event_queue.add_indirect_result(data)
        self._log.info(f"Close response listener thread on {self._public_addr}:{self._port}")
        self._socket.close()
        self._work_event.set()

    def stop(self):
        """
        Clear work event and wait until run method sets it again before exiting.
        This certifies that thread has finished.

        Since the thread listens on a socket with a time out of 0.5 seconds,
        the stopping can take up to 0.5 second in the worst case.
        """
        self._work_event.clear()
        self._work_event.wait()


class SubmitterThread(Thread):

    """
        The thread polls requests from work queue and submits them.
        After calling `run`, the thread runs in the background until `stop` is called.

        :param session_id: ID of active session
        :param service_name: name of FK deployment in cloud
    """

    def __init__(
        self,
        session_id: str,
        provider_client: ProviderClient,
        queue: WorkQueue,
        event_queue: EventQueue,
        response_handler: ResponseListener,
    ):
        super().__init__(daemon=True)
        self._session_id = session_id
        self._queue = queue
        self._event_queue = event_queue
        self._provider_client = provider_client
        self._response_handler = response_handler
        self._log = logging.getLogger("WorkerThread")
        self._work_event = Event()
        self._work_event.set()

        self.start()

    def stop(self):
        """
            Sets stop event and wait until run method clears it.
            This certifies that thread has finished.
        """
        self._work_event.clear()
        self._work_event.wait()

    # FIXME: batching of write requests
    def run(self):

        self._log.info(f"Begin submission worker thread.")

        while self._work_event.is_set():

            submission = self._queue.get()
            if not submission:
                continue

            req_id, request, future = submission
            try:
                if request.is_cloud_request():
                    """
                        Send the request to execution to the underlying cloud service.
                    """
                    self._log.info(f"Begin executing operation: {request.name}")
                    self._event_queue.add_expected_result(req_id, request, future)
                    self._provider_client.send_request(
                        request_id=f"{self._session_id}-{req_id}",
                        data={
                            **request.generate_request(),
                            "sourceIP": self._response_handler.address,
                            "sourcePort": self._response_handler.port,
                        },
                    )
                else:
                    # FIXME launch on a pool - then it becomes expected result as well
                    try:
                        res = self._provider_client.execute_request(request)
                        self._event_queue.add_direct_result(req_id, res, future)
                    except Exception as e:
                        self._event_queue.add_direct_result(req_id, e, future)
            except ProviderException as e:
                self._event_queue.add_direct_result(req_id, e, future)
            except Exception as e:
                self._event_queue.add_direct_result(req_id, e, future)
                self._log.info(f"Finish executing operation: {request.name}")

        self._log.info(f"Close queue worker thread.")
        self._work_event.set()


class SorterThread(Thread):
    """
        The thread polls requests from the event queue,
        and sorts them while releasing results to the user.
        After calling `run`, the thread runs in the background until `stop` is called.

        :param session_id: ID of active session
        :param service_name: name of FK deployment in cloud
    """

    def __init__(self, queue: EventQueue):
        super().__init__(daemon=True)
        self._queue = queue
        self._log = logging.getLogger("SorterThread")
        self._work_event = Event()
        self._work_event.set()

        self.start()

    def stop(self):
        """
            Sets stop event and wait until run method clears it.
            This certifies that thread has finished.
        """
        self._work_event.clear()
        self._work_event.wait()

    def _check_timeout(self, futures: list):

        cur_timestamp = datetime.now().timestamp()
        i = 0
        while i < len(futures):
            fut = futures[i]
            fut_timestamp = fut[-1]
            # timeout!
            if cur_timestamp - fut_timestamp >= 5.0:
                fut[2].set_exception(TimeoutException(5.0))
                # remove the element from the list
                futures.pop(0)
                i += 1
            else:
                break

    def run(self):

        self._log.info(f"Begin sorter thread.")

        futures = []
        #results = []

        while self._work_event.is_set():

            processed_result = False
            submission = self._queue.get()

            # FIXME: add timestamps to find missing events
            # if not event.wait(5.0):
            if not submission:
                self._check_timeout(futures)
                continue

            # we received result
            if submission[0] == EventQueue.EventType.CLOUD_EXPECTED_RESULT:
                futures.append((*submission[1:], datetime.now().timestamp()))
            # we have a direct result
            elif submission[0] == EventQueue.EventType.CLOUD_DIRECT_RESULT:
                req_id, result, future = submission[1:]
                # FIXME: enforce ordering
                if isinstance(result, Exception):
                    future.set_exception(result)
                else:
                    future.set_result(result)
                processed_result = True
            elif submission[0] == EventQueue.EventType.CLOUD_INDIRECT_RESULT:

                result = submission[1]
                # event format is: {session_id}-{local_idx}
                req_id = int(result["event"].split("-")[1])
                # FIXME: enforce ordering
                assert futures[0][0] == req_id
                req_id, request, future, _ = futures.pop(0)
                request.process_result(result, future)
                processed_result = True

            # if we processed result, then timeout could not have happend
            if not processed_result:
                self._check_timeout(futures)

        self._log.info(f"Close queue worker thread.")
        self._work_event.set()
