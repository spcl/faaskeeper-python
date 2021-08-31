from typing import List, Optional, Set


class AWSDecoder:

    """
        Convert the DynamoDB representation of a list/set into a Pythonic one.
    """

    @staticmethod
    def _decode_aws_schema_impl(items) -> List[str]:
        result: List[str] = []
        for key, value in items.items():
            if key == "L":
                for item in value:
                    result.extend(AWSDecoder._decode_aws_schema_impl(item))  # noqa
            elif isinstance(value, list):
                result.extend(value)
            else:
                result.append(value)
        return result

    @staticmethod
    def _decode_aws_schema(items: dict) -> List[int]:
        res = AWSDecoder._decode_aws_schema_impl(items)
        return [int(x) for x in res]


class SystemCounter:

    """
        The system counter consists of a sequence of integers.
        Each one corresponds to the most recent modification on a parallel writer.

        We can store provider-native data or the raw data with integers.
        The former is useful when we don't want to perform conversions, e.g.,
        we read DynamoDB structure with counter and use it when writing to
        DynamoDB table with storage.
    """

    def __init__(self, provider_data: Optional[dict], version: Optional[List[int]]):
        self._provider_data = provider_data
        self._version = version

    @staticmethod
    def from_provider_schema(provider_data: dict):
        return SystemCounter(provider_data, None)

    @staticmethod
    def from_raw_data(counter_data: List[int]):
        return SystemCounter(None, counter_data)

    @property
    def version(self) -> dict:
        assert self._provider_data
        return self._provider_data

    def serialize(self) -> List[int]:
        if self._version is None:
            assert self._provider_data is not None
            self._version = AWSDecoder._decode_aws_schema(self._provider_data)
        return self._version


class EpochCounter:

    """
        The epoch counter is a set of non-duplicated integer values.
        Each one corresponds to a watch invocation.
    """

    def __init__(self, provider_data: Optional[dict], version: Optional[Set[int]]):
        self._provider_data = provider_data
        self._version = version

    @staticmethod
    def from_provider_schema(provider_data: dict):
        return EpochCounter(provider_data, None)

    @staticmethod
    def from_raw_data(counter_data: Set[int]):
        return EpochCounter(None, counter_data)

    @property
    def version(self) -> dict:
        assert self._provider_data
        return self._provider_data

    # JSON cannot accept a set
    def serialize(self) -> List[int]:
        if self._version is None:
            assert self._provider_data is not None
            self._version = set(AWSDecoder._decode_aws_schema(self._provider_data))
        return list(self._version)


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

    @system.setter
    def system(self, val: SystemCounter):
        self._system = val

    @property
    def epoch(self) -> EpochCounter:
        return self._epoch

    @epoch.setter
    def epoch(self, val: EpochCounter):
        self._epoch = val

    def serialize(self) -> dict:
        return {"system": self._system.serialize(), "epoch": self._epoch.serialize()}
