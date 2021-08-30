import base64
import struct
from abc import ABC, abstractmethod
from typing import Dict, List, Union, cast

import boto3

from faaskeeper.config import Config, StorageType
from faaskeeper.exceptions import AWSException, NodeDoesntExistException
from faaskeeper.node import Node
from faaskeeper.providers.provider import ProviderClient
from faaskeeper.version import EpochCounter, SystemCounter, Version


class DataReader(ABC):
    def __init__(self, deployment_name: str):
        self._deployment_name = deployment_name

    @abstractmethod
    def get_data(self, path: str) -> Node:
        pass


class S3Reader(DataReader):
    def __init__(self, cfg: Config):
        super().__init__(cfg.deployment_name)
        self._config = cfg
        self._s3 = boto3.client("s3")
        self._storage_name = f"faaskeeper-{cfg.deployment_name}-data"

    def get_data(self, path: str) -> Node:

        try:
            obj = self._s3.get_object(Bucket=self._storage_name, Key=path)
            data = obj["Body"].read()

            # parse DynamoDB storage of node data and counter values
            n = Node(path)
            # unpack always returns a tuple, even for a single element
            # first element tells us the number of integers defining counters
            counter_len = struct.unpack_from("I", data)[0]
            # now parse counter data
            # for each counter of N values, we store N + 1 4 byte integers
            # counter_len counter_0 counter_1 .... counter_{N-1}
            counter_data = struct.unpack_from(f"{counter_len}I", data, offset=4)

            # read 'created' counter
            # first pos is counter length, then counter data
            begin = 1
            end = begin + counter_data[0]
            sys = SystemCounter(cast(List[int], counter_data[begin:end]))
            begin = end + 1
            end = begin + counter_data[begin - 1]
            epoch = EpochCounter(set(counter_data[begin:end]))
            n.created = Version(sys, epoch)

            # read 'modified' counter
            begin = end + 1
            end = begin + counter_data[begin - 1]
            sys = SystemCounter(cast(List[int], counter_data[begin:end]))
            begin = end + 1
            end = begin + counter_data[begin - 1]
            epoch = EpochCounter(set(counter_data[begin:end]))
            n.modified = Version(sys, epoch)

            # first 4 byte integers define the counter structure.
            # the rest ist just data
            # black does correct formatting, flake8 has a bug - it triggers E203 violation
            n.data = data[(counter_len + 1) * 4 :]  # noqa

            return n
        except self._s3.exceptions.NoSuchKey:
            raise NodeDoesntExistException(path)
        except Exception as e:
            raise AWSException(f"Failure on AWS client on S3 bucket {self._config.deployment_name}-data: {str(e)}")


class DynamoReader(DataReader):
    def __init__(self, client, cfg: Config):
        super().__init__(cfg.deployment_name)
        self._config = cfg
        self._dynamodb = client

    @staticmethod
    def _decode_schema_impl(items) -> List[str]:
        result: List[str] = []
        for key, value in items.items():
            if key == "L":
                for item in value:
                    result.extend(*DynamoReader._decode_schema_impl(item))
            elif isinstance(value, list):
                result.extend(value)
            else:
                result.append(value)
        return result

    @staticmethod
    def _decode_schema(items: list) -> List[int]:
        res = DynamoReader._decode_schema_impl(items)
        return [int(x) for x in res]

    def get_data(self, path: str) -> Node:

        try:
            # FIXME: check return value
            ret = self._dynamodb.get_item(
                TableName=f"faaskeeper-{self._config.deployment_name}-data",
                Key=AWSClient._convert_items({"path": path}),
                ConsistentRead=True,
                ReturnConsumedCapacity="TOTAL",
            )
            if "Item" not in ret:
                raise NodeDoesntExistException(path)

            # parse DynamoDB storage of node data and counter values
            n = Node(path)
            n.created = Version(
                SystemCounter(self._decode_schema(ret["Item"]["cFxidSys"])),
                EpochCounter(set(self._decode_schema(ret["Item"]["cFxidEpoch"]))),
            )
            n.modified = Version(
                SystemCounter(self._decode_schema(ret["Item"]["mFxidSys"])),
                EpochCounter(set(self._decode_schema(ret["Item"]["mFxidEpoch"]))),
            )
            n.data = base64.b64decode(ret["Item"]["data"]["B"])

            return n
        except Exception as e:
            raise AWSException(f"Failure on AWS client on DynamoDB table faaskeeper-{self._config.deployment_name}-data: {str(e)}")


class AWSClient(ProviderClient):
    def __init__(self, cfg: Config):
        super().__init__(cfg)
        self._dynamodb = boto3.client("dynamodb", self._config.deployment_region)
        self._data_reader: DataReader
        if cfg.user_storage == StorageType.PERSISTENT:
            self._data_reader = S3Reader(cfg)
        elif cfg.user_storage == StorageType.KEY_VALUE:
            self._data_reader = DynamoReader(self._dynamodb, cfg)
        else:
            raise NotImplementedError()

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

    @staticmethod
    def _dynamodb_val(val):
        if isinstance(val, bytes):
            return val  # base64.b64encode(val)
        elif isinstance(val, list):
            return val
        else:
            return str(val)

    @staticmethod
    def _convert_items(items: dict) -> dict:
        return {key: {AWSClient._dynamodb_type(value): AWSClient._dynamodb_val(value)} for key, value in items.items()}

    def send_request(
        self, request_id: str, data: Dict[str, Union[str, bytes, int]],
    ):
        # FIXME: handle failure
        try:
            import uuid

            # FIXME: check return value
            self._dynamodb.put_item(
                TableName=f"faaskeeper-{self._config.deployment_name}-write-queue",
                Item=AWSClient._convert_items({**data, "key": f"{str(uuid.uuid4())[0:4]}", "timestamp": request_id}),
            )
        except Exception as e:
            raise AWSException(
                f"Failure on AWS client on DynamoDB table faaskeeper-{self._config.deployment_name}-write-queue: {str(e)}"
            )

    def get_data(self, path: str) -> Node:
        return self._data_reader.get_data(path)

    def register_session(self, session_id: str, source_addr: str, heartbeat: bool):

        # FIXME: handle potential conflicts?
        # FIXME: fix heartbeat - it should be a frequency, not bool
        try:
            self._dynamodb.put_item(
                TableName=f"faaskeeper-{self._config.deployment_name}-state",
                Item=AWSClient._convert_items(
                    # {"type": session_id, "addr": source_addr, "ephemerals": [], "heartbeat": heartbeat}
                    {"key": session_id, "addr": source_addr, "ephemerals": []}
                ),
                ReturnConsumedCapacity="TOTAL",
            )
        except Exception as e:
            raise AWSException(
                f"Failure on AWS client on DynamoDB table faaskeeper-{self._config.deployment_name}-write-queue: {str(e)}"
            )
