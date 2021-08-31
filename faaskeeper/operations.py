from abc import ABC, abstractmethod

from faaskeeper.exceptions import (
    BadVersionError,
    FaaSKeeperException,
    NodeDoesntExistException,
    NodeExistsException,
    SessionExpiredException,
)
from faaskeeper.node import Node
from faaskeeper.threading import Future
from faaskeeper.version import SystemCounter, Version


class Operation(ABC):
    """Abstract base class for all provider-agnostic operations submitted to FK instance.

    :param session_id: ID of current session
    :param path: path of target node
    """

    def __init__(self, session_id: str, path: str):
        self._session_id = session_id
        self._path = path

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def path(self) -> str:
        return self._path

    @property
    @abstractmethod
    def name(self) -> str:
        pass


class RequestOperation(Operation):
    """Base class for all operations submitted to FK work queue.

    :param session_id: ID of current session
    :param path: path of target node
    """

    def __init__(self, session_id: str, path: str):
        super().__init__(session_id, path)

    @abstractmethod
    def generate_request(self) -> dict:
        pass

    @abstractmethod
    def process_result(self, result: dict, fut: Future):
        pass

    def is_cloud_request(self) -> bool:
        return True


class DirectOperation(Operation):
    """Base class for all operations executed directly by the client library.

    :param session_id: ID of current session
    :param path: path of target node
    """

    def __init__(self, session_id: str, path: str):
        super().__init__(session_id, path)

    def is_cloud_request(self) -> bool:
        return False


class CreateNode(RequestOperation):
    def __init__(self, session_id: str, path: str, value: bytes, flags: int):
        super().__init__(session_id, path)
        self._value = value

    def generate_request(self) -> dict:
        return {
            "op": "create_node",
            "path": self._path,
            "user": self._session_id,
            "version": -1,
            "flags": 0,
            "data": self._value,
        }

    def process_result(self, result: dict, fut: Future):
        if result["status"] == "success":
            n = Node(path=result["path"])
            n.created = Version(SystemCounter.from_raw_data(result["system_counter"]), None)
            fut.set_result(n)
        else:
            if result["reason"] == "node_exists":
                fut.set_exception(NodeExistsException(result["path"]))
            else:
                fut.set_exception(FaaSKeeperException("unknown error"))

    def returns_directly(self) -> bool:
        return False

    @property
    def name(self) -> str:
        return "create"


class SetData(RequestOperation):
    def __init__(self, session_id: str, path: str, value: bytes, version: int):
        super().__init__(session_id, path)
        self._value = value
        self._version = version

    def generate_request(self) -> dict:
        return {
            "op": self.name,
            "path": self._path,
            "user": self._session_id,
            "data": self._value,
            "version": self._version,
        }

    def process_result(self, result: dict, fut: Future):
        if result["status"] == "success":
            n = Node(path=result["path"])
            n.modified = Version(SystemCounter.from_raw_data(result["modified_system_counter"]), None)
            fut.set_result(n)
        else:
            if result["reason"] == "update_failure":
                fut.set_exception(BadVersionError(self._version))
            elif result["reason"] == "node_doesnt_exist":
                fut.set_exception(NodeDoesntExistException(self._path))
            else:
                fut.set_exception(FaaSKeeperException("unknown error"))

    def returns_directly(self) -> bool:
        return False

    @property
    def name(self) -> str:
        return "set_data"


class GetData(DirectOperation):
    def __init__(self, session_id: str, path: str):
        super().__init__(session_id, path)

    @property
    def name(self) -> str:
        return "get_data"


class RegisterSession(DirectOperation):
    def __init__(self, session_id: str, source_addr: str, heartbeat: bool):
        super().__init__(session_id, "")
        self.source_addr = source_addr
        self.heartbeat = heartbeat

    @property
    def name(self) -> str:
        return "register_session"


class DeregisterSession(RequestOperation):
    def __init__(self, session_id: str):
        super().__init__(session_id, "")

    def generate_request(self) -> dict:
        return {
            "op": self.name,
            "session_id": self._session_id,
        }

    def process_result(self, result: dict, fut: Future):
        if result["status"] == "success":
            fut.set_result(result["session_id"])
        else:
            if result["reason"] == "session_does_not_exist":
                fut.set_exception(SessionExpiredException())
            else:
                fut.set_exception(FaaSKeeperException("unknown error"))

    @property
    def name(self) -> str:
        return "deregister_session"
