import logging
from typing import Dict, Union

import boto3

from faaskeeper.providers.provider import ProviderClient


class AWSClient(ProviderClient):
    def __init__(self, service_name: str, verbose: bool):
        super().__init__(service_name)
        self._dynamodb = boto3.client("dynamodb")

    @staticmethod
    def _dynamodb_type(val):
        if isinstance(val, str):
            return "S"
        elif isinstance(val, int):
            return "N"
        elif isinstance(val, bytes):
            return "B"

    @staticmethod
    def _dynamodb_val(val):
        if isinstance(val, bytes):
            return val
        else:
            return str(val)

    @staticmethod
    def _convert_items(items: dict) -> dict:
        return {
            key: {AWSClient._dynamodb_type(value): AWSClient._dynamodb_val(value)}
            for key, value in items.items()
        }

    def send_request(
        self, request_id: str, data: Dict[str, Union[str, bytes, int]],
    ):
        try:
            ret = self._dynamodb.put_item(
                TableName=f"{self._service_name}-write-queue",
                Item=AWSClient._convert_items(
                    {**data, "key": self._service_name, "timestamp": request_id}
                ),
            )
        except Exception as e:
            logging.error("Failure!")
            logging.error(e)

    def get_data(self, path: str):

        try:
            print(AWSClient._convert_items({"key": path}))
            ret = self._dynamodb.get_item(
                TableName=f"{self._service_name}-data",
                Key=AWSClient._convert_items({"path": path}),
                ConsistentRead=True,
                ReturnConsumedCapacity="TOTAL",
            )
            print(ret)
        except Exception as e:
            logging.error("Failure!")
            logging.error(e)
