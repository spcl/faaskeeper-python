import base64
from enum import Enum
from typing import List, Optional

from faaskeeper.version import Version


class NodeDataType(Enum):
    DATA = 0
    CREATED = 1
    MODIFIED = 2
    CHILDREN = 3


class Node:

    """
    This class represents the FaaSKeeper node.
    It is similar to the original ZooKeeper node, with the major
    difference in the definition of node version - we use two counters.
    """

    def __init__(self, path: str):
        self._path = path
        self._data: Optional[bytes] = None
        self._data_b64: Optional[str] = None
        self._children: Optional[List[str]] = None
        self._created_version: Optional[Version] = None
        self._modified_version: Optional[Version] = None
        self._flag: Optional[int] = None

    @property
    def flag(self) -> int:
        assert self._flag is not None
        return self._flag

    @flag.setter
    def flag(self, flag: int):
        self._flag = flag

    @property
    def path(self) -> str:
        return self._path

    @property
    def children(self) -> List[str]:
        assert self._children is not None
        return self._children

    @children.setter
    def children(self, children: List[str]):
        self._children = children

    @property
    def has_children(self) -> bool:
        return self._children is not None

    @property
    def has_data(self) -> bool:
        return self._data is not None or self._data_b64 is not None

    @property
    def data(self) -> bytes:
        if self._data is None:
            assert self._data_b64 is not None
            return base64.b64decode(self._data_b64)
        else:
            return self._data

    @data.setter
    def data(self, data: bytes):
        self._data = data

    @property
    def data_b64(self) -> str:
        assert self._data_b64 is not None
        return self._data_b64

    @data_b64.setter
    def data_b64(self, data: str):
        self._data_b64 = data

    @property
    def has_created(self) -> bool:
        return self._created_version is not None

    @property
    def created(self) -> Version:
        assert self._created_version is not None
        return self._created_version

    @created.setter
    def created(self, val: Version):
        self._created_version = val

    @property
    def modified(self) -> Version:
        assert self._modified_version is not None
        return self._modified_version

    @modified.setter
    def modified(self, val: Version):
        self._modified_version = val

    @property
    def has_modified(self) -> bool:
        return self._modified_version is not None

    def serialize(self) -> dict:
        data_dict = {"data": str(self._data)} if self._data else {}
        version_dict = {}
        if self._created_version:
            version_dict["version"] = {
                "created": self._created_version.serialize(),
            }
        if self._modified_version:
            if "version" not in version_dict:
                version_dict["version"] = {}
            version_dict["version"]["modified"] = self._modified_version.serialize()
        children_dict = {}
        if self._children is not None:
            children_dict["children"] = self._children
        return {"path": self._path, **data_dict, **version_dict, **children_dict}
