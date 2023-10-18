from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Union
from faaskeeper.config import Config
from faaskeeper.node import Node
from faaskeeper.operations import (
    DirectOperation,
    ExistsNode,
    GetChildren,
    GetData,
    RegisterSession,
)
from faaskeeper.watch import Watch, WatchCallbackType, WatchType


class ProviderClient(ABC):
    def __init__(self, cfg: Config):
        self._config = cfg

    @abstractmethod
    def get_data(
        self, path: str, watch: Optional[WatchCallbackType], listen_address: Tuple[str, int]
    ) -> Tuple[Node, Optional[Watch]]:
        pass

    @abstractmethod
    def send_request(
        self,
        request_id: str,
        data: Dict[str, Union[str, bytes, int]],
    ):
        pass

    @abstractmethod
    def exists(self, path: str) -> Tuple[Optional[Node], Optional[Watch]]:
        pass

    @abstractmethod
    def get_children(self, path: str, include_data: bool) -> Tuple[List[Node], Optional[Watch]]:
        pass

    @abstractmethod
    def register_session(self, session_id: str, sourceAddr: str, heartbeat: bool):
        pass

    @abstractmethod
    def register_watch(
        self, node: Node, watch_type: WatchType, watch: WatchCallbackType, listen_address: Tuple[str, int]
    ) -> Watch:
        pass

    def execute_request(self, op: DirectOperation, listen_address: Tuple[str, int]):
        if isinstance(op, GetData):
            return self.get_data(op.path, op.watch, listen_address)
        elif isinstance(op, ExistsNode):
            return self.exists(op.path)
        elif isinstance(op, GetChildren):
            return self.get_children(op.path, op.include_data)
        elif isinstance(op, RegisterSession):
            return self.register_session(op.session_id, op.source_addr, op.heartbeat)
        else:
            raise NotImplementedError()
