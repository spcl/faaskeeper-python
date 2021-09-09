from typing import Dict, Optional, Union

import boto3

from faaskeeper.config import Config, StorageType
from faaskeeper.exceptions import AWSException, NodeDoesntExistException
from faaskeeper.node import Node
from faaskeeper.providers.provider import ProviderClient
from faaskeeper.providers.serialization import DataReader, DynamoReader, S3Reader


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

    def send_request(
        self, request_id: str, data: Dict[str, Union[str, bytes, int]],
    ):
        # FIXME: handle failure
        try:
            import uuid

            # FIXME: check return value
            self._dynamodb.put_item(
                TableName=f"faaskeeper-{self._config.deployment_name}-write-queue",
                Item=DynamoReader._convert_items({**data, "key": f"{str(uuid.uuid4())[0:4]}", "timestamp": request_id}),
            )
        except Exception as e:
            raise AWSException(
                f"Failure on AWS client on DynamoDB table "
                f"faaskeeper-{self._config.deployment_name}-write-queue: {str(e)}"
            )

    def get_data(self, path: str) -> Node:
        node = self._data_reader.get_data(path)
        if node is not None:
            return node
        else:
            raise NodeDoesntExistException(path)

    def exists(self, path: str) -> Optional[Node]:
        return self._data_reader.get_data(path, full_data=False)

    def register_session(self, session_id: str, source_addr: str, heartbeat: bool):

        # FIXME: handle potential conflicts?
        # FIXME: fix heartbeat - it should be a frequency, not bool
        try:
            self._dynamodb.put_item(
                TableName=f"faaskeeper-{self._config.deployment_name}-users",
                Item=DynamoReader._convert_items({"user": session_id, "addr": source_addr, "ephemerals": []}),
                ReturnConsumedCapacity="TOTAL",
            )
        except Exception as e:
            raise AWSException(
                f"Failure on AWS client on DynamoDB table "
                f"faaskeeper-{self._config.deployment_name}-write-queue: {str(e)}"
            )
