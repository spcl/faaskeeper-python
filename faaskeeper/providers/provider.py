from abc import ABC, abstractmethod

from faaskeeper.operations import DirectOperation, GetData, RegisterSession


class ProviderClient(ABC):
    def __init__(self, service_name: str):
        self._service_name = service_name

    @abstractmethod
    def get_data(self, path: str):
        pass

    @abstractmethod
    def register_session(self, session_id: str, sourceAddr: str, heartbeat: bool):
        pass

    def execute_request(self, op: DirectOperation):
        if isinstance(op, GetData):
            return self.get_data(op.path)
        elif isinstance(op, RegisterSession):
            return self.register_session(op.session_id, op.source_addr, op.heartbeat)
        else:
            raise NotImplementedError()
