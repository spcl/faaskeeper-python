
import logging

import boto3

class AWSClient:

    def __init__(self, verbose: bool):
        self._dynamodb = boto3.client('dynamodb')

    def send_request(self, table: str, service_name: str, request_id: str, data: Dict[str, Any]):
        
