from typing import Optional

from faaskeeper.version import Version


class Node:

    """
        This class represents the FaaSKeeper node.
        It is similar to the original ZooKeeper node, with the major
        difference in the definition of node version - we use two counters.
    """

    def __init__(self, path: str, ephemeral=false: bool):
        self._path = path
        self._ephemeral = ephemeral
        self._data: Optional[bytes] = None
        self._created_version: Optional[Version] = None
        self._modified_version: Optional[Version] = None

    @property
    def path(self) -> str:
        return self._path

    @property
    def data(self) -> bytes:
        assert self._data is not None
        return self._data

    @property
    def ephemeral(self) -> bool:
        return self._ephemeral

    @data.setter
    def data(self, data: bytes):
        self._data = data

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
        return {"path": self._path, **data_dict, **version_dict}
