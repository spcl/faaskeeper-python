from typing import Tuple


class SystemCounter:

    """
        The system counter consists of a sequence of integers.
        Each one corresponds to the most recent modification on a parallel writer.
    """

    def __init__(self, version: Tuple[int]):
        self._version = version

    @property
    def version(self):
        return self._version

    def serialize(self) -> Tuple[int]:
        return self._version


class EpochCounter:

    """
        The epoch counter is a set of non-duplicated integer values.
        Each one corresponds to a watch invocation.
    """

    def __init__(self, version: Tuple[int]):
        self._version = version

    @property
    def version(self) -> Tuple[int]:
        return self._version

    def serialize(self) -> Tuple[int]:
        return self._version


class Version:

    """
        Current version of the node. The version expresses the most
        recent modification to the system, and the watch notifications that were
        being applied while the version was updated.
    """

    def __init__(self, system: SystemCounter, epoch: EpochCounter):
        self._system = system
        self._epoch = epoch

    @property
    def system(self) -> SystemCounter:
        return self._system

    @property
    def epoch(self) -> EpochCounter:
        return self._epoch

    def serialize(self) -> dict:
        if self._system and self._epoch:
            return {"system": self._system.serialize(), "epoch": self._epoch.serialize()}
        else:
            return {"system": "unknown", "epoch": "unknown"}
