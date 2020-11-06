import json
import logging
import socket
import urllib
from threading import Thread, Event
from collections import deque
from typing import Callable, Deque, Dict, Tuple

from faaskeeper.operations import Operation
from faaskeeper.threading import Future
from faaskeeper.providers.provider import ProviderClient


"""
    Queue is served by a single thread processing requests.
"""


class WorkQueue:
    def __init__(self):
        self._request_count = 0
        self._queue: Deque[Tuple[int, Operation, Future]] = deque()
        self._wait_event = Event()

    def add_request(self, op: Operation, fut: Future):
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


"""
    Handle watch notification events and replies from service.

    In the current implementation, callbacks block the current thread.

    FIXME: add handler thread
"""


class EventQueue:
    def __init__(self):
        self._queue: Deque[Tuple[int, dict]] = deque()
        self._outstanding_waits: Dict[int, Callable[[dict], None]] = {}

    def add_callback(self, event_id: int, callback: Callable[[dict], None]):
        self._outstanding_waits[event_id] = callback

    def add_event(self, event: dict):
        event_id = int(event["event"].split("-")[1])
        self._queue.append((event_id, event))
        callback = self._outstanding_waits.get(event_id)
        if callback:
            del self._outstanding_waits[event_id]
            callback(event)


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

        self.start()

    def run(self):

        self._socket.listen(5)

        with open("log", "w") as f:
            while True:

                conn, addr = self._socket.accept()
                f.flush()
                with conn:
                    logging.info(f"Connected with {addr}")
                    data = conn.recv(1024).decode()
                    self._event_queue.add_event(json.loads(data))
                    # print(data, file=f)
                    # f.flush()
                    break


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

        self.start()

    # FIXME: batching of write requests
    def run(self):

        event = Event()
        result: dict = {}

        def callback(response):
            nonlocal event, result
            print("Wake up!", response)
            event.set()
            event.clear()
            result = response

        while True:

            # FIXME: add addresses of handler thread
            if not self._queue.empty():
                req_id, request, future = self._queue.pop()
                self._event_queue.add_callback(req_id, callback)
                self._provider_client.send_request(
                    table=f"{self._service_name}-write-queue",
                    service_name=self._service_name,
                    request_id=f"{self._session_id}-{req_id}",
                    data={
                        **request.generate_request(),
                        "sourceIP": self._response_handler.address,
                        "sourcePort": self._response_handler.port,
                    },
                )
                event.wait()
                request.process_result(result, future)
                print("Woken up!")
                print(result)
            else:
                self._queue._wait_event.wait()
