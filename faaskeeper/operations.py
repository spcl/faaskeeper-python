import logging
from abc import ABC, abstractmethod
from typing import Optional

from faaskeeper.exceptions import (
    BadVersionError,
    FaaSKeeperException,
    NodeDoesntExistException,
    NodeExistsException,
    NotEmptyException,
    SessionExpiredException,
)
from faaskeeper.node import Node
from faaskeeper.threading import Future
from faaskeeper.version import SystemCounter, Version
from faaskeeper.watch import WatchCallbackType


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

    @staticmethod
    @abstractmethod
    def deserialize(request: dict) -> Optional["RequestOperation"]:
        pass


class DirectOperation(Operation):
    """Base class for all operations executed directly by the client library.

    :param session_id: ID of current session
    :param path: path of target node
    """

    def __init__(self, session_id: str, path: str, watch: Optional[WatchCallbackType] = None):
        super().__init__(session_id, path)
        self._watch = watch

    def is_cloud_request(self) -> bool:
        return False

    @property
    def watch(self) -> Optional[WatchCallbackType]:
        return self._watch


class CreateNode(RequestOperation):
    def __init__(self, session_id: str, path: str, value: Optional[bytes], flags: int):
        super().__init__(session_id, path)
        self._value = value
        self._value_encoded: Optional[str] = None
        self._flags = flags

    def generate_request(self) -> dict:
        return {
            "op": "create_node",
            "path": self._path,
            "session_id": self._session_id,
            "version": -1,
            "flags": self._flags,
            "data": self._value,
        }

    @staticmethod
    def deserialize(request: dict) -> Optional["CreateNode"]:

        try:
            session_id = request["session_id"]
            path = request["path"]
            value = bytes()
            flags = request["flags"]

            op = CreateNode(session_id, path, value, flags)

            if "data" in request:
                op.data_b64 = request["data"]

            return op
        except KeyError as e:
            logging.error(f"Failed to parse the event {request}, missing key {e}")
            return None

    def process_result(self, result: dict, fut: Future):
        if result["status"] == "success":
            n = Node(path=result["path"])
            n.created = Version(SystemCounter.from_raw_data(result["system_counter"]), None)
            fut.set_result(n)
        else:
            if result["reason"] == "node_exists":
                fut.set_exception(NodeExistsException(result["path"]))
            elif result["reason"] == "node_doesnt_exist":
                fut.set_exception(NodeDoesntExistException(result["path"]))
            elif result["reason"] == "update_not_committed":
                fut.set_exception(FaaSKeeperException("Update could not be applied"))
            else:
                fut.set_exception(FaaSKeeperException("unknown error"))

    def returns_directly(self) -> bool:
        return False

    @property
    def name(self) -> str:
        return "create"

    @property
    def data_b64(self) -> str:
        assert self._value_encoded is not None
        return self._value_encoded

    @data_b64.setter
    def data_b64(self, val: str):
        self._value_encoded = val


class SetData(RequestOperation):
    def __init__(self, session_id: str, path: str, value: bytes, version: int):
        super().__init__(session_id, path)
        self._version = version
        self._value = value
        self._value_encoded: Optional[str] = None

    def generate_request(self) -> dict:
        return {
            "op": self.name,
            "path": self._path,
            "session_id": self._session_id,
            "data": self._value,
            "version": self._version,
        }

    @staticmethod
    def deserialize(request: dict) -> Optional["SetData"]:

        try:
            session_id = request["session_id"]
            path = request["path"]
            value = bytes()
            version = request["version"]

            op = SetData(session_id, path, value, version)

            if "data" in request:
                op.data_b64 = request["data"]

            return op
        except KeyError as e:
            logging.error(f"Failed to parse the event {request}, missing key {e}")
            return None

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
            elif result["reason"] == "update_not_committed":
                fut.set_exception(FaaSKeeperException("Update could not be applied"))
            else:
                fut.set_exception(FaaSKeeperException("unknown error"))

    def returns_directly(self) -> bool:
        return False

    @property
    def name(self) -> str:
        return "set_data"

    @property
    def data_b64(self) -> str:
        assert self._value_encoded is not None
        return self._value_encoded

    @data_b64.setter
    def data_b64(self, val: str):
        self._value_encoded = val


class DeleteNode(RequestOperation):
    def __init__(self, session_id: str, path: str, version: int):
        super().__init__(session_id, path)
        self._version = version

    def generate_request(self) -> dict:
        return {
            "op": self.name,
            "path": self._path,
            "session_id": self._session_id,
            "version": self._version,
        }

    @staticmethod
    def deserialize(request: dict) -> Optional["DeleteNode"]:

        try:
            session_id = request["session_id"]
            path = request["path"]
            version = request["version"]

            op = DeleteNode(session_id, path, version)

            return op
        except KeyError as e:
            logging.error(f"Failed to parse the event {request}, missing key {e}")
            return None

    def process_result(self, result: dict, fut: Future):
        if result["status"] == "success":
            # function returns void
            fut.set_result(None)
        else:
            if result["reason"] == "update_failure":
                fut.set_exception(BadVersionError(self._version))
            elif result["reason"] == "node_doesnt_exist":
                fut.set_exception(NodeDoesntExistException(self._path))
            elif result["reason"] == "not_empty":
                fut.set_exception(NotEmptyException(self._path))
            elif result["reason"] == "update_not_committed":
                fut.set_exception(FaaSKeeperException("Update could not be applied"))
            else:
                fut.set_exception(FaaSKeeperException("unknown error"))

    def returns_directly(self) -> bool:
        return False

    @property
    def name(self) -> str:
        return "delete_node"


class GetData(DirectOperation):
    def __init__(self, session_id: str, path: str, watch: Optional[WatchCallbackType] = None):
        super().__init__(session_id, path, watch)

    @property
    def name(self) -> str:
        return "get_data"


class GetChildren(DirectOperation):
    def __init__(self, session_id: str, path: str, include_data: bool):
        super().__init__(session_id, path)
        self._include_data = include_data

    @property
    def name(self) -> str:
        return "get_children"

    @property
    def include_data(self) -> bool:
        return self._include_data


class ExistsNode(DirectOperation):
    def __init__(self, session_id: str, path: str):
        super().__init__(session_id, path)

    @property
    def name(self) -> str:
        return "exists"


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

    @staticmethod
    def deserialize(request: dict) -> Optional["DeregisterSession"]:

        try:
            session_id = request["session_id"]
            op = DeregisterSession(session_id)

            return op
        except KeyError:
            logging.error(f"Failed to parse the event {request}")
            return None

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
