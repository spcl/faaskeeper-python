
import uuid

from faaskeeper.aws import AWSClient

class FaaSKeeperClient:

    _providers = {
        "aws": AWSClient
    }

    def __init__(self, provider: str, service_name: str, verbose: bool = False):
        self._client_id = str(uuid.uuid4())[0:8]
        self._service_name = service_name
        self._session_id = None
        #self._writer_queue = []
        self._write_requests_count = 0
        self._provider_client = FaaSKeeperClient._providers[provider](verbose)

    def start(self):
        """
            1) Start thread handling replies from FK.
            2) Start heartbeat thread
            3) Add yourself to the FK service.
        """
        self._session_id = str(uuid.uuid4())[0:8]

    def stop(self):
        """
            Before shutdown:
            1) Wait for pending requests.
            2) Notify system about closure.
            3) Stop heartbeat thread
        """
        # notify service about closure
        self._session_id = None
        self._write_requests_count = 0

    # TODO: sequence nodes
    # TODO: ephemeral nodes
    # TODO: ACL
    # TODO: makepath
    def create(self, path: str, value: str = b"", acl: str = None,
            ephemeral: bool = False, sequence: bool = False,
            makepath: bool = False) -> str:
        self._provider_client.send_request(
            table=f"{self._service_name}-write_queue",
            name=self._service_name,
            request_id=f"{self._session_id}-{self._write_requests_count}",
            data = {
                op: "create_node",
                path: path,
                user: self._session_id,
                version: -1,
                flags: 0,
                data: value,
                source_ip: "192.168.0.10",
                sourcePort: 12000
            }
        )
        #self._writer_queue.append(f"{self._session_id}-{self._write_requests_count}")
        self._write_requests_count += 1


