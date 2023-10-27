from typing import Dict, List, Optional, Tuple, Union
from faaskeeper.config import Config, QueueType
from faaskeeper.node import Node
from faaskeeper.providers.provider import ProviderClient
from faaskeeper.watch import Watch, WatchCallbackType, WatchType
from faaskeeper.providers.serialization import S3Reader
from faaskeeper.stats import StorageStatistics
from faaskeeper.exceptions import NodeDoesntExistException, WatchSetFailureException
from google.cloud import datastore
from google.cloud import storage
from google.cloud import pubsub_v1
from datetime import datetime, timedelta

import logging
import json
import google.cloud.exceptions
import base64

BENCHMARKING = True

class GCPClient(ProviderClient):
    def __init__(self, cfg: Config):
        super().__init__(cfg)
        self._log = logging.getLogger("GCPClient")
        self._datastore = datastore.Client(project=self._config.provider_config.project_id, 
                                           database=self._config.provider_config.database)
        self._watch_table = f"faaskeeper-{self._config.deployment_name}-watch"
        self._session_table = f"faaskeeper-{self._config.deployment_name}-users"
        _storage_client = storage.Client()
        self._bucket = _storage_client.bucket(self._config.provider_config.bucket_name)

        # writer queue for now is pub/sub
        if cfg.writer_queue == QueueType.PUBSUB:
            batch_settings = pubsub_v1.types.BatchSettings(
                max_messages=10,  # default 100, now it is 10
                max_bytes= 1 * 1000 * 1000,  # default 1 MB, still 1 MB -> 1000 * 1000 KB
                max_latency=0.01,  # default 10 ms, now is 10ms
            )
            publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True) # enable FIFO
            self._publisher_client = pubsub_v1.PublisherClient(publisher_options=publisher_options, batch_settings= batch_settings)
        
            self._topic_id = "writer-queue-topic"
            self._project_id = self._config.provider_config.project_id
            self._topic_path = self._publisher_client.topic_path(self._project_id, self._topic_id)
        else:
            raise NotImplementedError()
    
    def get_data_helper(self, path, include_data: bool, include_children: bool) -> Optional[Node]:
        begin = datetime.now()
        blob = self._bucket.get_blob(path)
        if blob is not None:
            file_content = blob.download_as_bytes()
            end = datetime.now()
            if BENCHMARKING:
                StorageStatistics.instance().add_read_time(int((end - begin) / timedelta(microseconds=1))) # 1 entity read
            return S3Reader.deserialize(path, file_content, include_children, include_data)
        return None

    def get_data(self, path: str, watch_callback: Optional[WatchCallbackType], listen_address: Tuple[str, int]) -> Tuple[Node, Optional[Watch]]:
        # read data
        node = self.get_data_helper(path, True, True)
        if node is not None:
            watch: Optional[Watch] = None
            if watch_callback is not None:
                """
                refer to the watch registration in aws.py
                """
                watch = self.register_watch(node, WatchType.GET_DATA, watch_callback, listen_address)
                # second read
                second_read_node = self.get_data_helper(path, True, True)
                if second_read_node is None or node.modified.system != second_read_node.modified.system:
                    raise WatchSetFailureException(path)
                # FIXME: insert watch into queue
            return node, watch
        else:
            raise NodeDoesntExistException(path)   

    def send_request(
        self,
        request_id: str,
        data: Dict[str, Union[str, bytes, int]],
    ):
        begin = datetime.now()
        if self._config.writer_queue == QueueType.PUBSUB:
            data["timestamp"] = request_id
            if "data" in data:
                data["data"] = base64.b64encode(data["data"]).decode()
            payload = json.dumps(data).encode()
            _ = self._publisher_client.publish(
                self._topic_path, payload, data["session_id"]
            )

            end = datetime.now()
            if BENCHMARKING:
                StorageStatistics.instance().add_write_time(int((end - begin) / timedelta(microseconds=1)))

    def exists(self, path: str) -> Tuple[Optional[Node], Optional[Watch]]:
        node = self.get_data_helper(path, False, False)
        # TODO: add watch
        return node, None

    def get_children(self, path: str, include_data: bool) -> Tuple[List[Node], Optional[Watch]]:
        node = self.get_data_helper(path, include_data, True)
        # TODO: add watch
        return node, None

    def register_session(self, session_id: str, sourceAddr: str, heartbeat: bool):
        try:
            with self._datastore.transaction():
                key = self._datastore.key(self._session_table, session_id)
                node = datastore.Entity(key)
                node.update({
                    "addr": sourceAddr,
                    "ephemerals": []
                })

                self._datastore.put(node)
        except google.cloud.exceptions:
            raise google.cloud.exceptions.BadRequest(
                "Failure on registering sessions on Datastore Database"
            )

    def register_watch(self, node: Node, watch_type: WatchType, watch: WatchCallbackType, listen_address: Tuple[str, int]) -> Watch:
        item_names: Dict[WatchType, str] = {WatchType.GET_DATA: "getData"}

        watch_name = item_names.get(watch_type)
        assert watch_name
        data_version = node.modified.system.sum
        watch_data = [[data_version, listen_address[0], listen_address[1]]]

        try:
            with self._datastore.transaction():
                key = self._datastore.key(self._watch_table, node.path)
                node_info = self._datastore.get(key)
                if node_info is not None:
                    if watch_name in node_info:
                        node_info[watch_name] = node_info[watch_name] + watch_data
                    else:
                        node_info[watch_name] = watch_data
                    self._datastore.put(node_info)
                else:
                    # create one
                    node_info[watch_name] = watch_data
                    self._datastore.put(node_info)
                StorageStatistics.instance().add_write_units(1)
        except google.cloud.exceptions:
            raise google.cloud.exceptions.BadRequest(
                "Failure on registering the watch on Datastore Database"
            ) 

        return Watch(data_version, watch_type, watch)


