from typing import List, Optional


class StorageStatistics:

    _instance: Optional["StorageStatistics"] = None

    def __init__(self):
        self.reset()

    def reset(self):
        self._read_units = 0
        self._write_units = 0
        self._read_times: List[float] = []
        self._write_times: List[float] = []

    @staticmethod
    def instance():
        if StorageStatistics._instance is None:
            StorageStatistics._instance = StorageStatistics()
        return StorageStatistics._instance

    @property
    def read_units(self) -> int:
        return self._read_units

    def add_read_units(self, val: int):
        self._read_units += val

    @property
    def write_units(self) -> int:
        return self._write_units

    def add_write_units(self, val: int):
        self._write_units += val

    @property
    def read_times(self) -> List[float]:
        return self._read_times

    def add_read_time(self, val: float):
        self._read_times.append(val)

    @property
    def write_times(self) -> List[float]:
        return self._write_times

    def add_write_time(self, val: float):
        self._write_times.append(val)
