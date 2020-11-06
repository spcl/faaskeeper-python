import logging
from typing import Dict, Union

import boto3

from faaskeeper.providers.provider import ProviderClient


class AWSClient(ProviderClient):
    def __init__(self, verbose: bool):
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
        self,
        table: str,
        service_name: str,
        request_id: str,
        data: Dict[str, Union[str, bytes, int]],
    ):
        try:
            ret = self._dynamodb.put_item(
                TableName=table,
                Item=AWSClient._convert_items(
                    {**data, "key": service_name, "timestamp": request_id}
                ),
            )
        except Exception as e:
            logging.error("Failure!")
            logging.error(e)
