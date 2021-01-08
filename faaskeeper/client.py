import logging
import io
import uuid
import sys
from datetime import datetime
from typing import Optional

from faaskeeper.queue import WorkQueue, EventQueue, ResponseListener, WorkerThread
from faaskeeper.operations import (
    CreateNode,
    GetData,
    SetData,
    RegisterSession,
    DeregisterSession,
)
from faaskeeper.providers.aws import AWSClient
from faaskeeper.threading import Future
from faaskeeper.exceptions import MalformedInputException, SessionExpiredException, TimeoutException


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

        self._log = logging.getLogger("faaskeeper")
        self._log.propagate = False
        for handler in self._log.handlers:
            self._log.removeHandler(handler)
        if verbose:
            self._log.setLevel(logging.INFO)
        else:
            self._log.setLevel(logging.ERROR)
        self._log_handler = logging.StreamHandler(sys.stdout)
        self._log_handler.setLevel(logging.INFO)
        self._log.addHandler(self._log_handler)

    @property
    def session_id(self) -> Optional[str]:
        return self._session_id

    @property
    def session_status(self) -> str:
        return "CONNECTED" if self._session_id else "DISCONNECTED"

    @staticmethod
    def _sanitize_path(path: str):
        if not path.startswith("/"):
            raise MalformedInputException("Path must begin with /")
        if path.endswith("/"):
            raise MalformedInputException("Path must not end with /")

    # FIXME: exception for incorrect connection
    def start(self) -> str:
        if self._session_id:
            self._log.info("Close existing session")
            # ignore timeouts and problems here
            try:
                self.stop()
            except:
                pass
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
        addr = f"{self._response_handler.address}:{self._response_handler.port}"
        future = Future()
        self._work_queue.add_request(
            RegisterSession(session_id=self._session_id, source_addr=addr,), future,
        )
        future.get()
        self._log.info(
            f"[{str(datetime.now())}] (FaaSKeeperClient) Registered "
            f"session: {self._session_id}"
        )
        return self._session_id

    def stop(self):
        if self._session_id is None:
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
        try:
            future = Future()
            self._work_queue.add_request(
                DeregisterSession(session_id=self._session_id,), future,
            )
            self._work_queue.close()
            self._work_queue.wait_close(3)
            future.get()
            self._log.info(
                f"[{str(datetime.now())}] (FaaSKeeperClient) Deregistered "
                f"session: {self._session_id}"
            )

            self._event_queue.close()
            #self._response_handler.stop()
            # self._work_thread.join(3)
            # if self._response_handler.is_alive() or self._work_thread.is_alive():
            #    raise TimeoutException()
        except TimeoutException as e:
            self._log.error("Service unavailable, couldn't properly close session")
            raise e
        finally:
            self._session_id = None
            self._work_queue = None
            self._event_queue = None
            self._response_handler = None
            self._work_thread = None

        return "closed"

    def logs(self) -> str:
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
        if not self._session_id:
            raise SessionExpiredException()

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
    def get_data(self, path: str,) -> bytes:
        return self.get_data_async(path).get()

    def get_data_async(self, path: str,) -> Future:

        FaaSKeeperClient._sanitize_path(path)
        future = Future()
        self._work_queue.add_request(
            GetData(session_id=self.session_id, path=path), future,
        )
        return future

    def set_data(self, path: str, value: bytes = b"", version: int = -1) -> str:
        return self.set_data_async(path, value, version).get()

    def set_data_async(
        self, path: str, value: bytes = b"", version: int = -1
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
