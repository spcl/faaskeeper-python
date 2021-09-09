import logging
import uuid
from typing import Optional

from faaskeeper.config import CloudProvider, Config
from faaskeeper.exceptions import (
    MalformedInputException,
    SessionExpiredException,
    TimeoutException,
)
from faaskeeper.node import Node
from faaskeeper.operations import (
    CreateNode,
    DeregisterSession,
    ExistsNode,
    GetData,
    RegisterSession,
    SetData,
)
from faaskeeper.providers.aws import AWSClient
from faaskeeper.queue import (
    EventQueue,
    ResponseListener,
    SorterThread,
    SubmitterThread,
    WorkQueue,
)
from faaskeeper.threading import Future


class FaaSKeeperClient:
    """This class represents the client connection to a FaaSKeeper instance
    and encapsulates a session.

    :param provider: name of the cloud provider, currently supported only "aws"
    :param service_name: name of the FaaSKeeper deployment instance
    :param port: port used to listening for incoming packets, defaults to -1
    :param heartbeat: if true, then this client will request heartbeat messages from the service
    :param verbose: verbose output of all operations
    :param debug: full debug output of all operations
    """

    _providers = {CloudProvider.AWS: AWSClient}

    def __init__(
        self, cfg: Config, port: int = -1, heartbeat: bool = True, verbose: bool = False, debug: bool = False,
    ):
        self._client_id = str(uuid.uuid4())[0:8]
        self._config = cfg
        self._session_id: Optional[str] = None
        self._closing_down = False
        self._heartbeat = heartbeat
        self._provider_client = FaaSKeeperClient._providers[cfg.cloud_provider](cfg)
        self._port = port

        if debug and verbose:
            logging.basicConfig(
                level=logging.DEBUG,
                format="[%(asctime)s] (%(name)s - %(filename)s(%(funcName)s" ":%(lineno)s)) %(message)s",
            )
        elif verbose:
            logging.basicConfig(
                level=logging.INFO, format="[%(asctime)s] (%(name)s) %(message)s",
            )
        else:
            logging.basicConfig(
                level=logging.ERROR, format="[%(asctime)s] (%(name)s) %(message)s",
            )
        self._log = logging.getLogger("FaaSKeeperClient")

    @property
    def session_id(self) -> Optional[str]:
        """
        :returns: session id
        """
        return self._session_id

    @property
    def session_status(self) -> str:
        """
        :returns: text description of session status
        """
        return "CONNECTED" if self._session_id else "DISCONNECTED"

    @staticmethod
    def _sanitize_path(path: str):
        if not path.startswith("/"):
            raise MalformedInputException("Path must begin with /")
        if path.endswith("/"):
            raise MalformedInputException("Path must not end with /")

    # FIXME: exception for incorrect connection
    def start(self) -> str:
        """Establish a connection to FaaSKeeper and start a session.
        The function starts two background threads: worker to handle the submission
        of new requests, and an event handler to receive replies and propagate results
        in an asynchronous manner.

        :returns: the ID of new session
        :raises: ?
        """

        if self._session_id:
            self._log.info("Close existing session")
            # ignore timeouts and problems here
            try:
                self.stop()
            except Exception:
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
        self._work_thread = SubmitterThread(
            self._session_id, self._provider_client, self._work_queue, self._event_queue, self._response_handler
        )
        self._sorter_thread = SorterThread(self._event_queue)
        addr = f"{self._response_handler.address}:{self._response_handler.port}"
        future = Future()
        self._work_queue.add_request(
            RegisterSession(session_id=self._session_id, source_addr=addr, heartbeat=self._heartbeat != -1,), future,
        )
        future.get()
        self._log.info(f"Registered session: {self._session_id}")
        return self._session_id

    def stop(self):
        """Disconnect from FaaSKeeper and stop the session.
        Cleans resources and stops both background threads.

        :returns: the ID of new session
        :raises: ?
        """

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
            self._log.info(f" Deregistered session: {self._session_id}")

        except TimeoutException as e:
            self._log.error("Service unavailable, couldn't properly close session")
            raise e
        finally:
            self._event_queue.close()
            self._response_handler.stop()
            self._work_thread.stop()
            self._sorter_thread.stop()
            assert not (self._response_handler.is_alive() or self._work_thread.is_alive())
            self._session_id = None
            self._work_queue = None
            self._event_queue = None
            self._response_handler = None
            self._work_thread = None
            self._sorter_thread = None
            self._closing_down = False

        return "closed"

    def create(self, path: str, value: bytes = b"", ephemeral: bool = False, sequential: bool = False,) -> Node:
        """Create new node synchronously.

        :param path: node path
        :param value: node data encoded as bytes
        :param ephemeral: true when this node should be ephemeral
        :param sequential: true when this node should have sequential path
        :returns: the path of new node
        """
        return self.create_async(path, value, ephemeral, sequential).get()

    # FIXME: Document exceptions
    def create_async(self, path: str, value: bytes = b"", ephemeral: bool = False, sequential: bool = False,) -> Future:
        """Create new node in an asynchronous mode.

        :param path: node path
        :param value: node data encoded as bytes
        :param ephemeral: true when this node should be ephemeral
        :param sequential: true when this node should have sequential path
        :returns: future representing the operation and its result - the path of the new node
        """
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
            CreateNode(session_id=self._session_id, path=path, value=value, flags=flags), future,
        )
        return future

    # FIXME: add watch
    # FIXME: document exceptions
    def get_data(self, path: str) -> Node:
        """Retrieve user data from a node.

        :param path: node path
        :returns: user data as bytes
        """
        return self.get_data_async(path).get()

    # FIXME: add watch
    # FIXME: document exceptions
    def get_data_async(self, path: str) -> Future:
        """Retrieve user data in an asynchronous mode.

        :param path: node path
        :returns: future representing the operation and its result - user data as bytes
        """

        FaaSKeeperClient._sanitize_path(path)
        future = Future()
        assert self.session_id
        self._work_queue.add_request(
            GetData(session_id=self.session_id, path=path), future,
        )
        return future

    # FIXME: add watch
    # FIXME: document exceptions
    def exists(self, path: str) -> Optional[Node]:
        """Retrieve user data from a node.

        :param path: node path
        :returns: user data as bytes
        """
        return self.exists_async(path).get()

    # FIXME: add watch
    # FIXME: document exceptions
    def exists_async(self, path: str) -> Future:
        """Retrieve user data in an asynchronous mode.

        :param path: node path
        :returns: future representing the operation and its result - user data as bytes
        """

        FaaSKeeperClient._sanitize_path(path)
        future = Future()
        assert self.session_id
        self._work_queue.add_request(
            ExistsNode(session_id=self.session_id, path=path), future,
        )
        return future

    # FIXME: document exceptions
    # FIXME: conditonal updates based on user data
    def set_data(self, path: str, value: bytes = b"", version: int = -1) -> str:
        """Modify the user data in a node.

        :param path: node path
        :param values: new data to be written
        :param version: apply the modification only if current version agrees with the argument, defaults to -1
        :returns: confirmation and new node version.
        """
        return self.set_data_async(path, value, version).get()

    # FIXME: document exceptions
    # FIXME: conditonal updates based on user data
    def set_data_async(self, path: str, value: bytes = b"", version: int = -1) -> Future:
        """Modify the user data in a node in an asynchronous mode.

        :param path: node path
        :param values: new data to be written
        :param version: apply the modification only if current version agrees with the argument, defaults to -1
        :returns: future representing the operation and its result - confirmation and new node version
        """
        # FIXME: add exception classes
        if not self._session_id:
            raise RuntimeError()

        FaaSKeeperClient._sanitize_path(path)
        future = Future()
        self._work_queue.add_request(
            SetData(session_id=self._session_id, path=path, value=value, version=version), future,
        )
        return future
