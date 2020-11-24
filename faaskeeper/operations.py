from abc import ABC, abstractmethod

from faaskeeper.threading import Future


class Operation(ABC):
    def __init__(self, session_id: str, path: str):
        self._session_id = session_id
        self._path = path

    @property
    def path(self) -> str:
        return self._path


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

    # FIXME: exception type
    # FIXME: result type
    def process_result(self, result: dict, fut: Future):
        if result["status"] == "success":
            fut.set_result(self._path)
        else:
            fut.set_exception(RuntimeError(result["reason"]))

    def returns_directly(self) -> bool:
        return False


class GetData(DirectOperation):
    def __init__(self, session_id: str, path: str):
        super().__init__(session_id, path)
