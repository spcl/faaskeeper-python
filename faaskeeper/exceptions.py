class FaaSKeeperException(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)


class ProviderException(FaaSKeeperException):
    """Generic exception describing a problem signaled by the cloud provider.
    """
    def __init__(self, msg: str):
        super().__init__(msg)


class SessionClosingException(FaaSKeeperException):
    """Used for operation submitted while the session was closing.
    """
    def __init__(self):
        super().__init__("Illegal operation while session is closing")


class SessionExpiredException(FaaSKeeperException):
    """Used for operation submitted for an expired/closed session.
    """
    def __init__(self):
        super().__init__("Illegal operation on an expired session")


class TimeoutException(FaaSKeeperException):
    """Describes operation cancelled after waiting too long for a reply from FK.
    """
    def __init__(self, time: int):
        super().__init__(f"Operation timed out after {time} [s]!")


class AWSException(ProviderException):
    """Generic exception describing a problem signaled by the AWS services.
    """
    def __init__(self, msg: str):
        super().__init__(msg)

class ZooKeeperException(FaaSKeeperException):
    """Generic exception describing a violation of ZooKeeper semantics.
    """
    def __init__(self, msg: str):
        super().__init__(msg)


class NodeExistsException(ZooKeeperException):
    """Describes failed node creation operation.
    """
    def __init__(self, path: str):
        super().__init__(f"Node {path} exists!")


class BadVersionError(ZooKeeperException):
    """Describes unsuccesful update operation.
    """
    def __init__(self, version: int):
        super().__init__(
            f"Update failed: node does not exist or version {version} does not match!"
        )

class MalformedInputException(ZooKeeperException):
    """Operation has not been succesful because of incorrect input.
    """
    def __init__(self, msg: str):
        super().__init__(msg)
