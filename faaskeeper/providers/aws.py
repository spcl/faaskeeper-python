import base64
import json
import logging
from datetime import datetime, timedelta
from os.path import join
from typing import Dict, List, Optional, Tuple, Union

import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from faaskeeper.config import Config, QueueType, StorageType
from faaskeeper.exceptions import (
    AWSException,
    NodeDoesntExistException,
    WatchSetFailureException,
)
from faaskeeper.node import Node
from faaskeeper.providers.provider import ProviderClient
from faaskeeper.providers.serialization import DataReader, DynamoReader, S3Reader
from faaskeeper.stats import StorageStatistics
from faaskeeper.watch import Watch, WatchCallbackType, WatchType

BENCHMARKING = True


class AWSClient(ProviderClient):
    def __init__(self, cfg: Config):
        super().__init__(cfg)
        self._cfg = cfg
        self._log = logging.getLogger("AWSClient")
        self._dynamodb = boto3.client("dynamodb", self._config.deployment_region)
        self._watch_table = f"faaskeeper-{self._config.deployment_name}-watch"
        self._type_serializer = TypeSerializer()
        self._type_deserializer = TypeDeserializer()
        self._data_reader: DataReader

        if cfg.user_storage == StorageType.PERSISTENT:
            self._data_reader = S3Reader(cfg)
        elif cfg.user_storage == StorageType.KEY_VALUE:
            self._data_reader = DynamoReader(self._dynamodb, cfg)
        else:
            raise NotImplementedError()

        if cfg.writer_queue == QueueType.DYNAMODB:
            pass
        elif cfg.writer_queue == QueueType.SQS:
            self._msg_id = 0
            self._queue_name = f"faaskeeper-{self._config.deployment_name}-writer-sqs.fifo"
            self._sqs_client = boto3.client("sqs", self._config.deployment_region)
            response = self._sqs_client.get_queue_url(QueueName=self._queue_name)
            self._sqs_queue_url = response["QueueUrl"]
        else:
            raise NotImplementedError()

    def send_request(
        self,
        request_id: str,
        data: Dict[str, Union[str, bytes, int]],
    ):
        # FIXME: handle failure
        try:
            import uuid

            begin = datetime.now()

            if self._cfg.writer_queue == QueueType.SQS:

                # if "data" in data:
                #    binary_data = data["data"]
                #    del data["data"]
                #    attributes = {"data": {"BinaryValue": binary_data, "DataType": "Binary"}}
                # else:
                #    binary_data = b""
                #    attributes = {}
                # FIXME: seperate serialization
                payload = DynamoReader._convert_items(data)
                if "data" in payload:
                    payload["data"]["B"] = base64.b64encode(payload["data"]["B"]).decode()

                attributes: dict = {}
                # FIXME: use response
                self._sqs_client.send_message(
                    QueueUrl=self._sqs_queue_url,
                    MessageBody=json.dumps(payload),
                    MessageAttributes=attributes,
                    MessageGroupId="0",
                    MessageDeduplicationId=request_id,
                )
                end = datetime.now()
                if BENCHMARKING:
                    StorageStatistics.instance().add_write_time(int((end - begin) / timedelta(microseconds=1)))

            elif self._cfg.writer_queue == QueueType.DYNAMODB:

                # FIXME: check return value
                begin = datetime.now()
                ret = self._dynamodb.put_item(
                    TableName=f"faaskeeper-{self._config.deployment_name}-write-queue",
                    Item=DynamoReader._convert_items(
                        {**data, "key": f"{str(uuid.uuid4())[0:4]}", "timestamp": request_id}
                    ),
                    ReturnConsumedCapacity="TOTAL",
                )
                end = datetime.now()
                if BENCHMARKING:
                    StorageStatistics.instance().add_write_time(int((end - begin) / timedelta(microseconds=1)))
                    StorageStatistics.instance().add_write_units(ret["ConsumedCapacity"]["CapacityUnits"])

        except Exception as e:
            raise AWSException(f"Failure on AWS client when sending request: {str(e)}")

    def get_data(
        self, path: str, watch_callback: Optional[WatchCallbackType], listen_address: Tuple[str, int]
    ) -> Tuple[Node, Optional[Watch]]:
        node = self._data_reader.get_data(path)

        if node is not None:
            watch: Optional[Watch] = None
            if watch_callback is not None:
                """Watch registration.
                If distributor processed an update between first and second read,
                then we failed to set the watch.
                We set a watch with an older value and either it will be ignored,
                or it will generate a useless notification.

                In any case, we didn't set a watch on correct, most recent
                data.
                """
                watch = self.register_watch(node, WatchType.GET_DATA, watch_callback, listen_address)
                second_read_node = self._data_reader.get_data(path)
                if second_read_node is None or node.modified.system != second_read_node.modified.system:
                    raise WatchSetFailureException(path)
                # FIXME: insert watch into queue
            return node, watch
        else:
            raise NodeDoesntExistException(path)

    def exists(self, path: str) -> Tuple[Optional[Node], Optional[Watch]]:
        node = self._data_reader.get_data(path, include_data=False, include_children=False)
        # FIXME: register a watch
        return node, None

    def get_children(self, path: str, include_data: bool) -> Tuple[List[Node], Optional[Watch]]:
        node = self._data_reader.get_data(path, include_data=False)
        # FIXME: register a watch
        assert node
        children = []
        for child in node.children:
            n = self._data_reader.get_data(join(path, child), include_data=include_data)
            if n is not None:
                children.append(n)
        return children, None

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
                f"faaskeeper-{self._config.deployment_name}-users: {str(e)}"
            )

    def register_watch(
        self, node: Node, watch_type: WatchType, watch: WatchCallbackType, listen_address: Tuple[str, int]
    ):
        # FIXME: make watch work with SQS
        item_names: Dict[WatchType, str] = {WatchType.GET_DATA: "getData"}

        """
            Watch registration:
            - attempt to insert the given watch type with node value, returning watch ID
            - if it failed, then update the watch counter and create the list of watches
        """
        watch_name = item_names.get(watch_type)
        assert watch_name
        data_version = node.modified.system.sum
        watch_data = self._type_serializer.serialize([[data_version, listen_address[0], listen_address[1]]])

        try:
            ret = self._dynamodb.update_item(
                TableName=self._watch_table,
                # path to the node
                Key={"path": {"S": node.path}},
                UpdateExpression=f"SET {watch_name} = list_append(if_not_exists({watch_name}, :empty_list), :newItem)",
                ExpressionAttributeValues={":newItem": watch_data, ":empty_list": {"L": []}},
                ReturnValues="ALL_NEW",
                ReturnConsumedCapacity="TOTAL",
            )
            StorageStatistics.instance().add_write_units(ret["ConsumedCapacity"]["CapacityUnits"])
            # success, watch already exists
        except self._dynamodb.exceptions.ConditionalCheckFailedException as e:
            self._log.error(f"Failed watch creation! Reason: {e}")

        return Watch(data_version, watch_type, watch)
        # FIXME: disable until we verify that
        # try:
        #    ret = self._dynamodb.update_item(
        #        TableName=self._watch_table,
        #        # path to the node
        #        Key={"path": {"S": node.path}},
        #        UpdateExpression=f"SET {watch_name} = list_append({watch_name}, :newItem)",
        #        ExpressionAttributeValues={
        #            ":newItem": watch_data,
        #        },
        #        ConditionExpression=f"attribute_exists({watch_name})",
        #        ReturnValues="ALL_NEW",
        #        ReturnConsumedCapacity="TOTAL",
        #    )
        #    # success, watch already exists
        #    watch_id = self._type_deserializer.deserialize(ret["Attributes"][f"{watch_name}ID"])
        # except self._dynamodb.exceptions.ConditionalCheckFailedException:

        #    # watch doesn't exist yet!

        #    # increase ephemeral counter
        #    #ret = self._dynamodb.update_item(
        #    #    TableName=self._watch_table,
        #    #    Key={"path": {"S": "epoch-counter"}},
        #    #    UpdateExpression=f"ADD #D :inc",
        #    #    ExpressionAttributeNames={"#D": "counter"},
        #    #    ExpressionAttributeValues={":inc": {"N": "1"}},
        #    #    ReturnValues="ALL_NEW",
        #    #    ReturnConsumedCapacity="TOTAL",
        #    #)
        #    #new_value = ret["Attributes"]["counter"]

        #    # create the watch, but make sure that we don't overwrite
        #    ret = self._dynamodb.update_item(
        #        TableName=self._watch_table,
        #        # path to the node
        #        Key={"path": {"S": node.path}},
        #        #UpdateExpression=f"SET {watch_name}ID = :counter, {watch_name} = :newItem",
        #        UpdateExpression=f"SET {watch_name} = :newItem",
        #        ExpressionAttributeValues={
        #            ":newItem": watch_data,
        #            #":counter": new_value,
        #        },
        #        ConditionExpression=f"attribute_not_exists({watch_name})",
        #        ReturnValues="ALL_NEW",
        #        ReturnConsumedCapacity="TOTAL",
        #    )
        #    watch_id = self._type_deserializer.deserialize(new_value)
