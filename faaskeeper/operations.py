from abc import ABC, abstractmethod


class Operation(ABC):
    def __init__(self, session_id: str, path: str):
        self._session_id = session_id
        self._path = path

    @abstractmethod
    def generate_request(self) -> dict:
        pass


class CreateNode(Operation):
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
