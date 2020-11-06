from threading import Thread, Event
from collections import deque
from typing import Deque, Tuple

from faaskeeper.operations import Operation
from faaskeeper.providers.provider import ProviderClient


"""
    Queue is served by a single thread processing requests.
"""


class WorkQueue:
    def __init__(self):
        self._request_count = 0
        self._queue: Deque[Tuple[int, Operation]] = deque()
        self._wait_event = Event()

    def add_request(self, op: Operation):
        self._queue.append((self._request_count, op))
        self._request_count += 1
        # only if queue was empty
        if len(self._queue) == 1:
            self._wait_event.set()
            self._wait_event.clear()

    def empty(self) -> bool:
        return len(self._queue) == 0

    def pop(self) -> Tuple[int, Operation]:
        return self._queue.popleft()


# FIXME: add sesssion state - id, name, config
class WorkerThread(Thread):
    def __init__(
        self,
        session_id: str,
        service_name: str,
        provider_client: ProviderClient,
        queue: WorkQueue,
        response_handler
    ):
        super().__init__()
        self._session_id = session_id
        self._service_name = service_name
        self._queue = queue
        self._provider_client = provider_client
        self._response_handler = response_handler

        self.start()

    def run(self):
        while True:

            # FIXME: add addresses of handler thread
            if not self._queue.empty():
                req_id, request = self._queue.pop()
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
            else:
                self._queue._wait_event.wait()


"""
    Handle watch notification events and replies from service.
"""


class EventQueue:
    pass
