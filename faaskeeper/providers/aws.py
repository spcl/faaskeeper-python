import base64
from typing import Dict, List, Union

import boto3

from faaskeeper.config import Config
from faaskeeper.exceptions import AWSException
from faaskeeper.node import Node
from faaskeeper.providers.provider import ProviderClient
from faaskeeper.version import EpochCounter, SystemCounter, Version


class AWSClient(ProviderClient):
    def __init__(self, cfg: Config):
        super().__init__(cfg)
        self._dynamodb = boto3.client("dynamodb", self._config.deployment_region)

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

    @staticmethod
    def _decode_schema_impl(items) -> List[str]:
        result: List[str] = []
        for key, value in items.items():
            if key == "L":
                for item in value:
                    result.extend(*AWSClient._decode_schema_impl(item))
            elif isinstance(value, list):
                result.extend(value)
            else:
                result.append(value)
        return result

    @staticmethod
    def _decode_schema(items: list) -> List[int]:
        res = AWSClient._decode_schema_impl(items)
        return [int(x) for x in res]

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
                f"Failure on AWS client on DynamoDB table {self._config.deployment_name}-write-queue: {str(e)}"
            )

    def get_data(self, path: str) -> Node:

        try:
            # FIXME: check return value
            ret = self._dynamodb.get_item(
                TableName=f"faaskeeper-{self._config.deployment_name}-data",
                Key=AWSClient._convert_items({"path": path}),
                ConsistentRead=True,
                ReturnConsumedCapacity="TOTAL",
            )

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
            raise AWSException(
                f"Failure on AWS client on DynamoDB table {self._config.deployment_name}-write-queue: {str(e)}"
            )

    def register_session(self, session_id: str, source_addr: str, heartbeat: bool):

        # FIXME: handle potential conflicts?
        # FIXME: fix heartbeat - it should be a frequency, not bool
        try:
            self._dynamodb.put_item(
                TableName=f"faaskeeper-{self._config.deployment_name}-state",
                Item=AWSClient._convert_items(
                    # {"type": session_id, "addr": source_addr, "ephemerals": [], "heartbeat": heartbeat}
                    {"type": session_id, "addr": source_addr, "ephemerals": []}
                ),
                ReturnConsumedCapacity="TOTAL",
            )
        except Exception as e:
            raise AWSException(
                f"Failure on AWS client on DynamoDB table {self._config.deployment_name}-write-queue: {str(e)}"
            )
