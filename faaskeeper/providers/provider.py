from abc import ABC, abstractmethod

from faaskeeper.operations import DirectOperation, GetData


class ProviderClient(ABC):
    def __init__(self, service_name: str):
        self._service_name = service_name

    @abstractmethod
    def get_data(self, path: str):
        pass

    def execute_request(self, op: DirectOperation):
        if isinstance(op, GetData):
            return self.get_data(op.path)
        else:
            raise NotImplementedError()
