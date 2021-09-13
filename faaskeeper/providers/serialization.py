import struct
from abc import ABC, abstractmethod
from functools import reduce
from typing import List, Optional, Set, cast

import boto3
from boto3.dynamodb.types import TypeDeserializer

from faaskeeper.config import Config
from faaskeeper.exceptions import AWSException
from faaskeeper.node import Node
from faaskeeper.version import EpochCounter, SystemCounter, Version


class DataReader(ABC):
    def __init__(self, deployment_name: str):
        self._deployment_name = deployment_name

    @abstractmethod
    def get_data(self, path: str, include_data: bool = True, include_children: bool = True) -> Optional[Node]:
        pass


class S3Reader(DataReader):
    def __init__(self, cfg: Config):
        super().__init__(cfg.deployment_name)
        self._config = cfg
        self._s3 = boto3.client("s3")
        self._storage_name = f"faaskeeper-{cfg.deployment_name}-data"

    @staticmethod
    def serialize(node: Node) -> bytes:
        """
            Binary data format
            int: total header size (in bytes)
            int: total number of integers
            int: number of integers in created system
            int[]: created system
            int: number of integers in created epoch
            int[]: created epoch
            int: number of integers in modified system
            int[]: modified system
            int: number of integers in modified epoch
            int[]: modified epoch

            int: total number of strings (children)
            The latter is repeated for each string:
            int: size of string
            char[]: string data
        """

        created_system = node.created.system.serialize()
        created_epoch: Set[int] = set()
        modified_system = node.modified.system.serialize()
        modified_epoch: Set[int] = set()
        children = [x.encode() for x in node.children]
        children_lengths = [len(x) for x in children]

        # first pack counters
        counters = [created_system, created_epoch, modified_system, modified_epoch]
        total_length = reduce(lambda a, b: a + b, map(len, counters))
        data = struct.pack(
            f"<{5+total_length+2}I",
            4 * (5 + total_length + 2) + 4 * len(children) + sum(children_lengths),
            4 + total_length,
            len(created_system),
            *created_system,
            len(created_epoch),
            *created_epoch,
            len(modified_system),
            *modified_system,
            len(modified_epoch),
            *modified_epoch,
            len(children),
        )

        # now pack strings
        format_string = "<"
        for child in children:
            format_string += f"1I{len(child)}s"
        data += struct.pack(format_string, *[y for x in zip(children_lengths, children) for y in x])
        return data + node.data

    def deserialize(path: str, data: bytes, include_data: bool = True, include_children: bool = True) -> Node:

        # parse DynamoDB storage of node data and counter values
        n = Node(path)
        # unpack always returns a tuple, even for a single element
        # first element tells us the entire header size
        # the second one - number of integers defining counters
        header_size, counter_len = struct.unpack_from("<2I", data)
        offset = struct.calcsize("<2I")
        # now parse counter data
        # for each counter of N values, we store N + 1 4 byte integers
        # counter_len counter_0 counter_1 .... counter_{N-1}
        counter_data = struct.unpack_from(f"<{counter_len}I", data, offset=offset)
        offset += struct.calcsize(f"<{counter_len}I")

        # read 'created' counter
        # first pos is counter length, then counter data
        begin = 1
        end = begin + counter_data[0]
        sys = SystemCounter.from_raw_data(cast(List[int], counter_data[begin:end]))
        begin = end + 1
        end = begin + counter_data[begin - 1]
        epoch = EpochCounter.from_raw_data(set(counter_data[begin:end]))
        n.created = Version(sys, epoch)

        # read 'modified' counter
        begin = end + 1
        end = begin + counter_data[begin - 1]
        sys = SystemCounter.from_raw_data(cast(List[int], counter_data[begin:end]))
        begin = end + 1
        end = begin + counter_data[begin - 1]
        epoch = EpochCounter.from_raw_data(set(counter_data[begin:end]))
        n.modified = Version(sys, epoch)

        if include_children:
            num_children_strings = struct.unpack_from(f"<I", data, offset=offset)[0]
            offset += struct.calcsize(f"<I")

            strings = []
            # now read the encoded strings
            # unfortunately, there's no native support for variable len strings
            # we read number of strings
            # then we read string length & follow with reading string data
            for i in range(num_children_strings):
                str_len = struct.unpack_from("<I", data, offset=offset)[0]
                offset += struct.calcsize(f"<I")
                string_data = struct.unpack_from(f"<{str_len}s", data, offset=offset)[0]
                offset += struct.calcsize(f"<{str_len}s")
                strings.append(string_data.decode())
            n.children = strings

        if include_data:
            # first 4 byte integers define the counter structure.
            # the rest ist just data
            # black does correct formatting, flake8 has a bug - it triggers E203 violation
            n.data = data[offset:]  # noqa

        return n

    def get_data(self, path: str, include_data: bool = True, include_children: bool = True) -> Optional[Node]:

        try:
            obj = self._s3.get_object(Bucket=self._storage_name, Key=path)
            data = obj["Body"].read()

            return self.deserialize(path, data, include_data, include_children)
        except self._s3.exceptions.NoSuchKey:
            return None
        except Exception as e:
            raise AWSException(
                f"Failure on AWS client on S3 bucket faaskeeper-{self._config.deployment_name}-data: {str(e)}"
            )


class DynamoReader(DataReader):
    def __init__(self, client, cfg: Config):
        super().__init__(cfg.deployment_name)
        self._config = cfg
        self._dynamodb = client
        self._deserializer = TypeDeserializer()

    # FIXME: replace with boto3 serialization
    @staticmethod
    def _dynamodb_type(val):
        if isinstance(val, str):
            return "S"
        elif isinstance(val, int):
            return "N"
        elif isinstance(val, bytes):
            return "B"
        elif isinstance(val, list):
            return "L"
        # tuple is represented as "L" also
        elif isinstance(val, tuple):
            return "L"

    # FIXME: replace with boto3 serialization
    @staticmethod
    def _dynamodb_val(val):
        if isinstance(val, bytes):
            return val  # base64.b64encode(val)
        elif isinstance(val, list):
            return val
        else:
            return str(val)

    # FIXME: replace with boto3 serialization
    @staticmethod
    def _convert_items(items: dict) -> dict:
        return {
            key: {DynamoReader._dynamodb_type(value): DynamoReader._dynamodb_val(value)} for key, value in items.items()
        }

    def get_data(self, path: str, include_data: bool = True, include_children: bool = True) -> Optional[Node]:

        try:
            # FIXME: check return value
            AttributesToGet = ["cFxidSys", "cFxidEpoch", "mFxidSys", "mFxidEpoch"]
            if include_data:
                AttributesToGet.append("data")
            if include_children:
                AttributesToGet.append("children")
            ret = self._dynamodb.get_item(
                TableName=f"faaskeeper-{self._config.deployment_name}-data",
                Key=DynamoReader._convert_items({"path": path}),
                ConsistentRead=True,
                ReturnConsumedCapacity="TOTAL",
                AttributesToGet=AttributesToGet,
            )
        except Exception as e:
            raise AWSException(
                f"Failure on AWS client on DynamoDB table faaskeeper-{self._config.deployment_name}-data: {str(e)}"
            )
        if "Item" not in ret:
            return None

        # parse DynamoDB storage of node data and counter values
        n = Node(path)
        n.created = Version(
            SystemCounter.from_provider_schema(ret["Item"]["cFxidSys"]),
            EpochCounter.from_provider_schema(ret["Item"]["cFxidEpoch"]),
        )
        n.modified = Version(
            SystemCounter.from_provider_schema(ret["Item"]["mFxidSys"]),
            EpochCounter.from_provider_schema(ret["Item"]["mFxidEpoch"]),
        )
        if "data" in ret["Item"]:
            n.data = ret["Item"]["data"]["B"]
        if "children" in ret["Item"]:
            n.children = self._deserializer.deserialize(ret["Item"]["children"])

        return n
