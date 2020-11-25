from abc import ABC, abstractmethod

from faaskeeper.threading import Future
from faaskeeper.exceptions import FaaSKeeperException, NodeExistsException


class Operation(ABC):
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
    def __init__(self, session_id: str, path: str):
        super().__init__(session_id, path)

    def is_cloud_request(self) -> bool:
        return False


class CreateNode(RequestOperation):
    def __init__(self, session_id: str, path: str, value: bytes, acl: int, flags: int):
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
            fut.set_result(result["path"])
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


class GetData(DirectOperation):
    def __init__(self, session_id: str, path: str):
        super().__init__(session_id, path)

    @property
    def name(self) -> str:
        return "get_data"

class RegisterSession(DirectOperation):
    def __init__(self, session_id: str, source_addr: str):
        super().__init__(session_id, '')
        self.source_addr = source_addr

    @property
    def name(self) -> str:
        return "register_session"

class DeregisterSession(RequestOperation):
    def __init__(self, session_id: str):
        super().__init__(session_id, '')

    def generate_request(self) -> dict:
        return {
            "op": self.name,
            "user": self._session_id,
        }

    def process_result(self, result: dict, fut: Future):
        if result["status"] == "success":
            fut.set_result(result["path"])
        else:
            if result["reason"] == "node_exists":
                fut.set_exception(NodeExistsException(result["path"]))
            else:
                fut.set_exception(FaaSKeeperException("unknown error"))

    @property
    def name(self) -> str:
        return "deregister_session"

