from enum import Enum


class StorageType(Enum):
    PERSISTENT = 0
    KEY_VALUE = 1
    IN_MEMORY = 2

    @staticmethod
    def deserialize(val: str) -> "StorageType":
        return {"persistent": StorageType.PERSISTENT, "key-value": StorageType.KEY_VALUE}[val]


class QueueType(Enum):
    DYNAMODB = 0
    SQS = 1

    @staticmethod
    def deserialize(val: str) -> "QueueType":
        return {"dynamodb": QueueType.DYNAMODB, "sqs": QueueType.SQS}[val]


class ClientChannel(Enum):
    TCP = 0
    SQS = 1

    @staticmethod
    def deserialize(val: str) -> "ClientChannel":
        return {"tcp": ClientChannel.TCP, "sqs": ClientChannel.SQS}[val]


class CloudProvider(Enum):
    AWS = 0

    @staticmethod
    def serialize(val: "CloudProvider") -> str:
        return {CloudProvider.AWS: "aws"}[val]

    @staticmethod
    def deserialize(val: str) -> "CloudProvider":
        return {"aws": CloudProvider.AWS}[val]


class AWSConfig:
    def __init__(self):
        self._data_bucket: str

    @property
    def data_bucket(self) -> str:
        return self._data_bucket

    @staticmethod
    def deserialize(data: dict) -> "AWSConfig":
        cfg = AWSConfig()
        cfg._data_bucket = data["data-bucket"]
        return cfg


class Config:
    def __init__(self):
        self._verbose: bool
        self._provider: CloudProvider
        self._region: str
        self._deployment_name: str
        self._heartbeat_frequency: int
        self._user_storage: StorageType
        self._writer_queue: QueueType
        self._provider_cfg: AWSConfig
        self._client_channel: ClientChannel

    @property
    def verbose(self) -> bool:
        return self._verbose

    @property
    def cloud_provider(self) -> CloudProvider:
        return self._provider

    @property
    def deployment_region(self) -> str:
        return self._region

    @property
    def deployment_name(self) -> str:
        return self._deployment_name

    @property
    def heartbeat_frequency(self) -> int:
        return self._heartbeat_frequency

    @property
    def user_storage(self) -> StorageType:
        return self._user_storage

    @property
    def writer_queue(self) -> QueueType:
        return self._writer_queue

    @property
    def provider_config(self) -> AWSConfig:
        return self._provider_cfg

    @property
    def client_channel(self) -> ClientChannel:
        return self._client_channel

    @staticmethod
    def deserialize(data: dict) -> "Config":
        cfg = Config()
        cfg._verbose = data["verbose"]
        cfg._provider = CloudProvider.deserialize(data["cloud-provider"])
        cfg._region = data["deployment-region"]
        cfg._deployment_name = data["deployment-name"]
        cfg._heartbeat_frequency = data["heartbeat-frequency"]
        cfg._user_storage = StorageType.deserialize(data["user-storage"])
        cfg._writer_queue = QueueType.deserialize(data["worker-queue"])
        cfg._client_channel = ClientChannel.deserialize(data["client-channel"])

        if cfg._provider == CloudProvider.AWS:
            cfg._provider_cfg = AWSConfig.deserialize(data["aws"])
        else:
            raise NotImplementedError()

        return cfg
