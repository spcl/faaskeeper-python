from typing import Dict, Union

import boto3

from faaskeeper.node import Node
from faaskeeper.version import Version, SystemCounter, EpochCounter
from faaskeeper.providers.provider import ProviderClient
from faaskeeper.exceptions import AWSException


class AWSClient(ProviderClient):
    def __init__(self, service_name: str, region: str, verbose: bool):
        super().__init__(service_name, region)
        self._dynamodb = boto3.client("dynamodb", region)

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
        if isinstance(val, bytes) or isinstance(val, list):
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
                TableName=f"{self._service_name}-write-queue",
                Item=AWSClient._convert_items(
                    {**data, "key": f"self._service_name_{str(uuid.uuid4())[0:4]}", "timestamp": request_id}
                ),
            )
        except Exception as e:
            raise AWSException(f"Failure on AWS client on DynamoDB table {self._service_name}-write-queue: {str(e)}")

    def get_data(self, path: str) -> Node:

        try:
            # FIXME: check return value
            ret = self._dynamodb.get_item(
                TableName=f"{self._service_name}-data",
                Key=AWSClient._convert_items({"path": path}),
                ConsistentRead=True,
                ReturnConsumedCapacity="TOTAL",
            )

            # parse DynamoDB storage of node data and counter values
            n = Node(path)
            n.created = Version(SystemCounter(ret["Item"]["cFxidSys"]), EpochCounter(ret["Item"]["cFxidEpoch"]))
            n.modified = Version(SystemCounter(ret["Item"]["mFxidSys"]), EpochCounter(ret["Item"]["mFxidEpoch"]))
            n.data = ret["Item"]["data"]["B"].decode()

            return n
        except Exception as e:
            raise AWSException(f"Failure on AWS client on DynamoDB table {self._service_name}-write-queue: {str(e)}")

    def register_session(self, session_id: str, source_addr: str, heartbeat: bool):

        # FIXME: handle potential conflicts?
        # FIXME: fix heartbeat - it should be a frequency, not bool
        try:
            self._dynamodb.put_item(
                TableName=f"{self._service_name}-state",
                Item=AWSClient._convert_items(
                    # {"type": session_id, "addr": source_addr, "ephemerals": [], "heartbeat": heartbeat}
                    {"type": session_id, "addr": source_addr, "ephemerals": []}
                ),
                ReturnConsumedCapacity="TOTAL",
            )
        except Exception as e:
            raise AWSException(f"Failure on AWS client on DynamoDB table {self._service_name}-write-queue: {str(e)}")
