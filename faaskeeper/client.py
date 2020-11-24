import uuid
from typing import Optional

from faaskeeper.queue import WorkQueue, EventQueue, ResponseListener, WorkerThread
from faaskeeper.operations import CreateNode
from faaskeeper.providers.aws import AWSClient
from faaskeeper.threading import Future


class FaaSKeeperClient:

    _providers = {"aws": AWSClient}

    def __init__(
        self, provider: str, service_name: str, port: int = -1, verbose: bool = False
    ):
        self._client_id = str(uuid.uuid4())[0:8]
        self._service_name = service_name
        self._session_id = None
        self._provider_client = FaaSKeeperClient._providers[provider](verbose)
        self._port = port

    @property
    def session_id(self) -> Optional[str]:
        return self._session_id

    def start(self):
        """
            1) Start thread handling replies from FK.
            2) Start heartbeat thread
            3) Add yourself to the FK service.
        """
        self._session_id = str(uuid.uuid4())[0:8]
        self._work_queue = WorkQueue()
        self._event_queue = EventQueue()
        self._response_handler = ResponseListener(self._event_queue, self._port)
        self._work_thread = WorkerThread(
            self._session_id,
            self._service_name,
            self._provider_client,
            self._work_queue,
            self._response_handler,
            self._event_queue,
        )

    def stop(self):
        """
            Before shutdown:
            1) Wait for pending requests.
            2) Notify system about closure.
            3) Stop heartbeat thread
        """
        # notify service about closure
        self._session_id = None

    # TODO: ACL
    def create(
        self,
        path: str,
        value: bytes = b"",
        acl: str = None,
        ephemeral: bool = False,
        sequential: bool = False,
    ) -> str:
        return self.create_async(path, value, acl, ephemeral, sequential).get()

    def create_async(
        self,
        path: str,
        value: bytes = b"",
        acl: str = None,
        ephemeral: bool = False,
        sequential: bool = False,
    ) -> Future:
        # FIXME: add exception classes
        if not self._session_id:
            raise RuntimeError()
        flags = 0
        if ephemeral:
            flags |= 1
        if sequential:
            flags |= 2

        future = Future()
        self._work_queue.add_request(
            CreateNode(
                session_id=self._session_id, path=path, value=value, acl=0, flags=flags
            ),
            future
        )
        return future
