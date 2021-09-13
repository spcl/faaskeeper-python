from enum import Enum
from typing import Callable

from faaskeeper.node import Node


class WatchEventType(Enum):
    """Implementing API similar to ZooKeeper - inspired by the
        org.apache.zookeeper.Watcher.Event.EventType
    """

    NODE_CREATED = 0
    NODE_DELETED = 1
    NODE_DATA_CHANGED = 2
    NODE_CHILDREN_CHANGED = 3


class WatchedEvent:

    """Implementing API similar to ZooKeeper - inspired by the
        org.apache.zookeeper.WatchedEvent
    """

    def __init__(self, event_type: "WatchEventType", node: Node):
        self._node = node
        self._event_type = event_type

    @property
    def node(self) -> Node:
        return self._node

    @property
    def event_type(self) -> "WatchEventType":
        return self._event_type


WatchCallbackType = Callable[[WatchedEvent], None]


class WatchType(Enum):
    GET_DATA = 0
    EXISTS = 1
    GET_CHILDREN = 2


class Watch:
    def __init__(self, watch_id: int, watch_type: WatchType):
        self._watch_id = watch_id
        self._watch_type = watch_type

    def generate_message(self, msg: dict):
        # FIXME: parse cloud event into proper message
        return msg
