import logging
import io
import os
import uuid
from datetime import datetime
from typing import Optional, List

from faaskeeper.queue import WorkQueue, EventQueue, ResponseListener, WorkerThread
from faaskeeper.operations import CreateNode, GetData, SetData, RegisterSession, DeregisterSession
from faaskeeper.providers.aws import AWSClient
from faaskeeper.threading import Future
from faaskeeper.exceptions import TimeoutException, MalformedInputException


class FaaSKeeperClient:

    _providers = {"aws": AWSClient}

    def __init__(
        self, provider: str, service_name: str, port: int = -1, verbose: bool = False
    ):
        self._client_id = str(uuid.uuid4())[0:8]
        self._service_name = service_name
        self._session_id = None
        self._closing_down = False
        self._provider_client = FaaSKeeperClient._providers[provider](
            service_name, verbose
        )
        self._port = port

        self._log_stream = io.StringIO()
        self._log = logging.getLogger('faaskeeper')
        self._log.propagate = False
        for handler in self._log.handlers:
            self._log.removeHandler(handler)
        self._log.setLevel(logging.INFO)
        self._log_handler = logging.StreamHandler(self._log_stream)
        self._log_handler.setLevel(logging.INFO)
        self._log.addHandler(self._log_handler)

    @property
    def session_id(self) -> Optional[str]:
        return self._session_id

    @staticmethod
    def _sanitize_path(path: str):
        if not path.startswith('/'):
            raise MalformedInputException("Path must begin with /")
        if path.endswith('/'):
            raise MalformedInputException("Path must not end with /")

    # FIXME: exception for incorrect connection
    def start(self):
        """
            1) Start thread handling replies from FK.
            2) Start heartbeat thread
            3) Add ourself to the FK service.
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
        future = Future()
        self._work_queue.add_request(
            RegisterSession(
                session_id=self._session_id,
                source_addr=f"{self._response_handler.address}:{self._response_handler.port}"
            ),
            future,
        )
        future.get()
        self._log.info(f"[{str(datetime.now())}] (FaaSKeeperClient) Registered session: {self._session_id}")

    def stop(self):
        if self._session_id == None:
            return "closed"
        if self._closing_down:
            return "closing in progress"

        """
            Before shutdown:
            1) Notify system about closure.
            2) Notify queue that we're closing
            3) Wait for pending requests.
            4) Verify that we're correctly closed
            4) Stop heartbeat thread
        """
        self._closing_down = True
        future = Future()
        self._work_queue.add_request(
            DeregisterSession(
                session_id=self._session_id,
            ),
            future,
        )
        self._work_queue.close()
        self._work_queue.wait_close(3)
        future.get()
        self._log.info(f"[{str(datetime.now())}] (FaaSKeeperClient) Deregistered session: {self._session_id}")

        self._event_queue.close()
        # FIXME: close threads
        #self._response_handler.join(3)
        #self._work_thread.join(3)
        #if self._response_handler.is_alive() or self._work_thread.is_alive():
        #    raise TimeoutException()

        self._session_id = None
        self._work_queue = None
        self._event_queue = None
        self._response_handler = None
        self._work_thread = None

        return "closed"


    def logs(self) -> List[str]:
        self._log_handler.flush()
        return self._log_stream.getvalue()

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

        FaaSKeeperClient._sanitize_path(path)
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
            future,
        )
        return future

    # FIXME: watch
    # FIXME: stat
    def get_data(
        self,
        path: str,
    ) -> bytes:
        return self.get_data_async(path).get()

    def get_data_async(
        self,
        path: str,
    ) -> Future:

        FaaSKeeperClient._sanitize_path(path)
        future = Future()
        self._work_queue.add_request(
            GetData(
                session_id=self._session_id, path=path
            ),
            future,
        )
        return future

    def set_data(
        self,
        path: str,
        value: bytes = b"",
        version: int = -1
    ) -> str:
        return self.set_data_async(path, value, version).get()

    def set_data_async(
        self,
        path: str,
        value: bytes = b"",
        version: int = -1
    ) -> Future:
        # FIXME: add exception classes
        if not self._session_id:
            raise RuntimeError()

        FaaSKeeperClient._sanitize_path(path)
        future = Future()
        self._work_queue.add_request(
            SetData(
                session_id=self._session_id, path=path, value=value, version=version
            ),
            future,
        )
        return future
