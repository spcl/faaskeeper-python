from functools import total_ordering
from typing import Dict, List, Optional, Set

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer


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


@total_ordering
class SystemCounter:

    """
        The system counter consists of a sequence of integers.
        Each one corresponds to the most recent modification on a parallel writer.

        We can store provider-native data or the raw data with integers.
        The former is useful when we don't want to perform conversions, e.g.,
        we read DynamoDB structure with counter and use it when writing to
        DynamoDB table with storage.

        We implement comparison and inequality operators to compare counters.
    """

    def __init__(self, provider_data: Optional[dict], version: Optional[List[int]]):
        self._provider_data = provider_data
        self._version = version
        self._sum = 0

    def _compute_sum(self):
        if self._sum == 0:
            self._sum = sum(self.serialize())
        return self._sum

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

    @property
    def sum(self) -> int:
        return self._compute_sum()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SystemCounter):
            return NotImplemented
        return self._compute_sum() == other._compute_sum()

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, SystemCounter):
            return NotImplemented
        return self._compute_sum() < other._compute_sum()

    def serialize(self) -> List[int]:
        if self._version is None:
            assert self._provider_data is not None
            self._version = AWSDecoder._decode_aws_schema(self._provider_data)
        return self._version


class EpochCounter:

    _type_deserializer = TypeDeserializer()
    _type_serializer = TypeSerializer()

    """
        The epoch counter is a set of non-duplicated integer values.
        Each one corresponds to a watch invocation.
    """

    def __init__(self, provider_data: Optional[dict], version: Optional[Set[str]]):
        self._provider_data = provider_data
        self._version = version

    @staticmethod
    def from_provider_schema(provider_data: dict):
        return EpochCounter(provider_data, None)

    @staticmethod
    def from_raw_data(counter_data: Set[str]):
        return EpochCounter(None, counter_data)

    @property
    def version(self) -> dict:
        # FIXME: this should be hidden under abstraction
        if self._provider_data is None:
            self._provider_data = EpochCounter._type_serializer.serialize(self._version)
        return self._provider_data

    # JSON cannot accept a set
    def serialize(self) -> List[str]:
        if self._version is None:
            assert self._provider_data is not None
            self._version = set(EpochCounter._type_deserializer.deserialize(self._provider_data))
            # self._version = set(AWSDecoder._decode_aws_schema(self._provider_data))
        return list(self._version)


class Version:

    """
        Current version of the node. The version expresses the most
        recent modification to the system, and the watch notifications that were
        being applied while the version was updated.

        The epoch counter is not used by writer function, thus it can be null
        at the global stage. It is set only in the user region.
    """

    def __init__(self, system: SystemCounter, epoch: Optional[EpochCounter]):
        self._system = system
        self._epoch = epoch

    @property
    def system(self) -> SystemCounter:
        return self._system

    @system.setter
    def system(self, val: SystemCounter):
        self._system = val

    @property
    def epoch(self) -> Optional[EpochCounter]:
        return self._epoch

    @epoch.setter
    def epoch(self, val: EpochCounter):
        self._epoch = val

    def serialize(self) -> dict:
        sys: Dict[str, list] = {"system": self._system.serialize()}
        if self._epoch is not None:
            sys["epoch"] = self._epoch.serialize()
        return sys
