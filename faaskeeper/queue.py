import json
import logging
import socket
import time
import urllib
from datetime import datetime
from threading import Thread, Event
from collections import deque
from typing import Callable, Deque, Dict, Tuple

from faaskeeper.operations import Operation
from faaskeeper.threading import Future
from faaskeeper.exceptions import ProviderException, TimeoutException, SessionClosingException
from faaskeeper.providers.provider import ProviderClient


def wait_until(condition, timeout: int, interval: float = 0.1, *args):
    start = time.time()
    while not condition(*args) and time.time() - start < timeout:
        time.sleep(interval)

"""
    Queue is served by a single thread processing requests.
"""


class WorkQueue:
    def __init__(self):
        self._request_count = 0
        self._queue: Deque[Tuple[int, Operation, Future]] = deque()
        self._wait_event = Event()
        self._closing = False

    def add_request(self, op: Operation, fut: Future):
        if self._closing:
            raise SessionClosingException()
        self._queue.append((self._request_count, op, fut))
        self._request_count += 1
        # only if queue was empty
        if len(self._queue) == 1:
            self._wait_event.set()
            self._wait_event.clear()

    def empty(self) -> bool:
        return len(self._queue) == 0

    def pop(self) -> Tuple[int, Operation, Future]:
        return self._queue.popleft()

    def close(self):
        self._closing = True

    def wait_close(self, timeout: int = -1):
        if timeout > 0:
            wait_until(self.empty, timeout)
            if not self.empty():
                raise TimeoutException(timeout)


"""
    Handle watch notification events and replies from service.

    In the current implementation, callbacks block the current thread.

"""


class EventQueue:
    def __init__(self):
        self._queue: Deque[Tuple[int, dict]] = deque()
        self._outstanding_waits: Dict[int, Callable[[dict], None]] = {}
        self._closing = False

    def add_callback(self, event_id: int, callback: Callable[[dict], None]):
        self._outstanding_waits[event_id] = callback

    def add_event(self, event: dict):
        if self._closing:
            raise SessionClosingException()

        event_id = int(event["event"].split("-")[1])
        self._queue.append((event_id, event))
        callback = self._outstanding_waits.get(event_id)
        if callback:
            del self._outstanding_waits[event_id]
            callback(event)

    def empty(self) -> bool:
        return len(self._queue) == 0

    def close(self):
        self._closing = True

    def wait_close(self, timeout: int = -1):
        if timeout > 0:
            wait_until(self.empty, timeout)
            if not self.empty():
                raise TimeoutException(timeout)


class ResponseListener(Thread):
    @property
    def address(self):
        return self._public_addr

    @property
    def port(self):
        return self._port

    def __init__(self, event_queue: EventQueue, port: int = -1):

        super().__init__(daemon=True)
        self._event_queue = event_queue

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind(("", port if port != -1 else 0))

        req = urllib.request.urlopen("https://checkip.amazonaws.com")
        self._public_addr = req.read().decode().strip()
        self._port = self._socket.getsockname()[1]
        self._log = logging.getLogger('faaskeeper')

        self.start()

    def run(self):

        self._socket.listen(5)

        while True:

            conn, addr = self._socket.accept()
            with conn:
                logging.info(f"Connected with {addr}")
                data = json.loads(conn.recv(1024).decode())
                self._log.info(f"[{str(datetime.now())}] (ResponseListener) Received message: {data}")
                self._event_queue.add_event(data)

# FIXME: add sesssion state - id, name, config
class WorkerThread(Thread):
    def __init__(
        self,
        session_id: str,
        service_name: str,
        provider_client: ProviderClient,
        queue: WorkQueue,
        response_handler: ResponseListener,
        event_queue: EventQueue,
    ):
        super().__init__(daemon=True)
        self._session_id = session_id
        self._service_name = service_name
        self._queue = queue
        self._event_queue = event_queue
        self._provider_client = provider_client
        self._response_handler = response_handler
        self._log = logging.getLogger('faaskeeper')

        self.start()

    # FIXME: batching of write requests
    def run(self):

        event = Event()
        result: dict = {}

        def callback(response):
            nonlocal event, result
            event.set()
            event.clear()
            result = response

        while True:

            # FIXME: add addresses of handler thread
            if not self._queue.empty():
                req_id, request, future = self._queue.pop()

                """
                    Send the request to execution to the underlying cloud service,
                    register yourself with an event queue and wait until response arrives.
                """
                self._log.info(f"[{str(datetime.now())}] (WorkerThread) Begin executing operation: {request.name}")
                if request.is_cloud_request():
                    try:
                        self._event_queue.add_callback(req_id, callback)
                        self._provider_client.send_request(
                            request_id=f"{self._session_id}-{req_id}",
                            data={
                                **request.generate_request(),
                                "sourceIP": self._response_handler.address,
                                "sourcePort": self._response_handler.port,
                            },
                        )
                        if not event.wait(5.0):
                            future.set_exception(TimeoutException(5.0))
                            continue
                        request.process_result(result, future)
                    except ProviderException as e:
                        future.set_exception(e)
                else:
                    try:
                        res = self._provider_client.execute_request(request)
                        future.set_result(res)
                    except Exception as e:
                        future.set_exception(e)
                self._log.info(f"[{str(datetime.now())}] (WorkerThread) Finish executing operation: {request.name}")

            else:
                self._queue._wait_event.wait()

