from enum import Enum
from typing import Callable


class WatchEventType(Enum):
    """Implementing API similar to ZooKeeper - inspired by the
    org.apache.zookeeper.Watcher.Event.EventType
    """

    NODE_CREATED = 0
    NODE_DELETED = 1
    NODE_DATA_CHANGED = 2
    NODE_CHILDREN_CHANGED = 3

    def __str__(self):
        return {0: "NodeCreated", 1: "NodeDeleted", 2: "NodeDataChanged", 3: "NodeChildrenChanged"}.get(self.value)


class WatchedEvent:

    """Implementing API similar to ZtooKeeper - inspired by the
    org.apache.zookeeper.WatchedEvent
    """

    def __init__(self, event_type: "WatchEventType", path: str, timestamp: int):
        self._path = path
        self._event_type = event_type
        self._timestamp = timestamp

    @property
    def timestamp(self) -> int:
        return self._timestamp

    @property
    def path(self) -> str:
        return self._path

    @property
    def event_type(self) -> "WatchEventType":
        return self._event_type


WatchCallbackType = Callable[[WatchedEvent], None]


class WatchType(Enum):
    GET_DATA = 0
    EXISTS = 1
    GET_CHILDREN = 2


class Watch:
    def __init__(self, timestamp: int, watch_type: WatchType, callback: WatchCallbackType):
        self._timestamp = timestamp
        self._watch_type = watch_type
        self._callback = callback

    def generate_message(self, event: WatchedEvent):
        self._callback(event)

    @property
    def watch_type(self) -> "WatchType":
        return self._watch_type

    @property
    def timestamp(self) -> int:
        return self._timestamp
