

class FaaSKeeperException(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)

class ProviderException(FaaSKeeperException):
    def __init__(self, msg: str):
        super().__init__(msg)

class AWSException(ProviderException):
    def __init__(self, msg: str):
        super().__init__(msg)

class TimeOutException(FaaSKeeperException):
    def __init__(self, time: int):
        super().__init__(f"Operation timed out after {time} [s]!")

class ZooKeeperException(FaaSKeeperException):
    def __init__(self, msg: str):
        super().__init__(msg)

class NodeExistsException(ZooKeeperException):
    def __init__(self, path: str):
        super().__init__(f"Node {path} exists!")

