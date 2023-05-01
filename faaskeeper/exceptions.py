class FaaSKeeperException(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)


class ProviderException(FaaSKeeperException):
    """Generic exception describing a problem signaled by the cloud provider."""

    def __init__(self, msg: str):
        super().__init__(msg)


class SessionClosingException(FaaSKeeperException):
    """Used for operation submitted while the session was closing."""

    def __init__(self):
        super().__init__("Illegal operation while session is closing")


class SessionExpiredException(FaaSKeeperException):
    """Used for operation submitted for an expired/closed session."""

    def __init__(self):
        super().__init__("Illegal operation on an expired session")


class TimeoutException(FaaSKeeperException):
    """Describes operation cancelled after waiting too long for a reply from FK."""

    def __init__(self, time: float):
        super().__init__(f"Operation timed out after {time} [s]!")


class WatchSetFailureException(FaaSKeeperException):
    """Describes failed watch set because of an update that happened in between."""

    def __init__(self, path: str):
        super().__init__(f"Failed to set a watch on node: {path}!")


class AWSException(ProviderException):
    """Generic exception describing a problem signaled by the AWS services."""

    def __init__(self, msg: str):
        super().__init__(msg)


class ZooKeeperException(FaaSKeeperException):
    """Generic exception describing a violation of ZooKeeper semantics."""

    def __init__(self, msg: str):
        super().__init__(msg)


class NodeExistsException(ZooKeeperException):
    """Describes failed node creation operation."""

    def __init__(self, path: str):
        super().__init__(f"Node {path} exists!")


class NodeDoesntExistException(ZooKeeperException):
    """Describes failed node read operation."""

    def __init__(self, path: str):
        super().__init__(f"Node {path} does not exist!")


class NotEmptyException(ZooKeeperException):
    """Describes failed delete operation because of existing children."""

    def __init__(self, path: str):
        super().__init__(f"Node {path} cannot be deleted - it has children!")


class BadVersionError(ZooKeeperException):
    """Describes unsuccesful update operation."""

    def __init__(self, version: int):
        super().__init__(f"Update failed: node does not exist or version {version} does not match!")


class MalformedInputException(ZooKeeperException):
    """Operation has not been succesful because of incorrect input."""

    def __init__(self, msg: str):
        super().__init__(msg)
