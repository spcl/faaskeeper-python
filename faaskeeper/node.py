from typing import Optional

from faaskeeper.version import Version


class Node:

    """
        This class represents the FaaSKeeper node.
        It is similar to the original ZooKeeper node, with the major
        difference in the definition of node version - we use two counters.
    """

    def __init__(self, path: str):
        self._path = path
        self._data: Optional[bytes] = None
        self._created_version: Optional[Version] = None
        self._modiifed_version: Optional[Version] = None

    @property
    def path(self) -> str:
        return self._path

    @property
    def data(self) -> bytes:
        assert self._data
        return self._data

    @data.setter
    def data(self, data: bytes):
        self._data = data

    @property
    def created(self) -> Version:
        assert self._created_version
        return self._created_version

    @created.setter
    def created(self, val: Version):
        self._created_version = val

    @property
    def modified(self) -> Version:
        assert self._modified_version
        return self._modified_version

    @modified.setter
    def modified(self, val: Version):
        self._modified_version = val

    def serialize(self) -> dict:
        assert self._created_version
        assert self._modified_version
        return {
            "path": self._path,
            "data": str(self._data),
            "version": {"created": self._created_version.serialize(), "modified": self._modified_version.serialize()},
        }
