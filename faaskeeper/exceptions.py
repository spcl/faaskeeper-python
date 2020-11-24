

class FaaSKeeperException(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)

class ProviderException(FaaSKeeperException):
    def __init__(self, msg: str):
        super().__init__(msg)

class AWSException(ProviderException):
    def __init__(self, msg: str):
        super().__init__(msg)

