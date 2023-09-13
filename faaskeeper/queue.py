import hashlib
import json
import logging
import select
import socket
import time
import urllib.request
from datetime import datetime
from enum import Enum
from queue import Empty, Queue
from threading import Event, Lock, Thread
from typing import Dict, List, Optional, Tuple, Union

import boto3
from botocore.exceptions import ClientError

from faaskeeper.config import Config
from faaskeeper.exceptions import (
    ProviderException,
    SessionClosingException,
    TimeoutException,
)
from faaskeeper.node import Node
from faaskeeper.operations import Operation
from faaskeeper.providers.provider import ProviderClient
from faaskeeper.threading import Future
from faaskeeper.watch import Watch, WatchedEvent, WatchEventType, WatchType


def wait_until(timeout: float, interval: float, condition, *args):
    """A simple hack to wait for an event until a specified length of time passes.

    :param timeout: time to wait for a result [seconds]
    :param interval: sleep time - defines how frequently we check for a result [seconds]
    :param condition: event condition to be evaluated
    :param args: arguments passed to the condition function
    """
    start = time.time()
    while not condition(*args) and time.time() - start < timeout:
        time.sleep(interval)


"""
    Architecture of the receive queue systems on the client.
    We must handle three types of events:
    - direct results of reading from the storage.
    - results of operations returned from serverless workers.
    - watch events delivered.

    The event queue is not necessarily ordered and is used to to transmit results to the
    ordering thread.

    The submission queue is FIFO and is used to transmit requests to the submitter thread.
"""


class EventQueue:
    class EventType(Enum):
        CLOUD_INDIRECT_RESULT = 0
        CLOUD_DIRECT_RESULT = 1
        CLOUD_EXPECTED_RESULT = 2
        WATCH_NOTIFICATION = 3

    """
        The queue is used to handle replies and watch notifications from the service.
        Its second responsibility is ensuring that the results are correctly ordered.

        The queue is served by a single thread processing events.
        In the current implementation, callbacks block the only thread.
    """

    def __init__(self):
        self._queue = Queue()
        # Stores hash of node -> watches
        # User could have multiple watches per node (exists, get_data)
        self._watches: Dict[str, List[Watch]] = {}
        self._watches_lock = Lock()
        self._closing = False
        self._log = logging.getLogger("EventQueue")

    def add_expected_result(self, request_id: int, request: Operation, future: Future):
        if self._closing:
            raise SessionClosingException()

        self._queue.put((EventQueue.EventType.CLOUD_EXPECTED_RESULT, request_id, request, future))

    def add_direct_result(self, request_id: int, result: Union[Node, Exception], future: Future):
        if self._closing:
            raise SessionClosingException()

        self._queue.put((EventQueue.EventType.CLOUD_DIRECT_RESULT, request_id, result, future))

    def add_indirect_result(self, result: dict):
        if self._closing:
            raise SessionClosingException()

        self._queue.put((EventQueue.EventType.CLOUD_INDIRECT_RESULT, result))

    def add_watch_notification(self, result: dict):
        if self._closing:
            raise SessionClosingException()

        path = result["path"]
        watch_event = WatchEventType(result["watch-event"])
        timestamp = result["timestamp"]

        hashed_path = hashlib.md5(path.encode()).hexdigest()

        with self._watches_lock:
            existing_watches = self._watches.get(hashed_path)
            if existing_watches:
                watches_retain = []
                watches_removed_num = 0
                for w in existing_watches:
                    if w.timestamp < timestamp:
                        # deliver all prev watches this is partial removal
                        self._queue.put(
                            (
                                EventQueue.EventType.WATCH_NOTIFICATION,
                                w,
                                WatchedEvent(watch_event, path, timestamp),
                            )
                        )
                        watches_removed_num += 1

                    else:
                        watches_retain.append(w)
                if watches_removed_num == len(existing_watches):
                    self._watches.pop(hashed_path, None)
                else:
                    self._watches[hashed_path] = watches_retain
                return
            else:
                self._log.warn(f"Ignoring unknown watch notification for even {watch_event} on path {path}")

    def add_watch(self, path: str, watch: Watch):
        if self._closing:
            raise SessionClosingException()

        # verify that we don't replace watches
        with self._watches_lock:
            hashed_path = hashlib.md5(path.encode()).hexdigest()
            existing_watches = self._watches.get(hashed_path)
            if existing_watches:
                for idx, w in enumerate(existing_watches):
                    # Replace existing watch
                    # FIXME: is it safe? shouldn't we just generate notification?
                    # this means that we read result before getting notification
                    if w.watch_type == watch.watch_type:
                        existing_watches[idx] = watch
                        return
                # watch doesn't exist yet
                self._watches[hashed_path].append(watch)
            else:
                self._watches[hashed_path] = [watch]

    # FIXME: find by watch type?
    # get only watches older than timestamp - avoid getting watch that we
    # just set a moment ago
    def get_watches(self, paths: List[str], timestamp: int) -> List[Watch]:
        # DO not remove from queue, just query from it.
        if self._closing:
            raise SessionClosingException()

        # verify that we don't replace watches
        watches_to_deliver = []
        with self._watches_lock:
            for p in paths:
                existing_watches = self._watches.get(p, [])
                for w in existing_watches:
                    if w.timestamp < timestamp:
                        watches_to_deliver.append(w)
        return watches_to_deliver

    def get(self) -> Optional[Tuple]:
        try:
            return self._queue.get(block=True, timeout=0.5)
        except Empty:
            return None

    def close(self):
        self._closing = True


class WorkQueue:
    def __init__(self):
        self._queue = Queue()
        self._closing = False
        self._request_count = 0

    def add_request(self, op: Operation, fut: Future):
        if self._closing:
            raise SessionClosingException()

        self._queue.put((self._request_count, op, fut))
        self._request_count += 1

    def get(self) -> Optional[Tuple[int, Operation, Future]]:
        try:
            return self._queue.get(block=True, timeout=0.5)
        except Empty:
            return None

    def close(self):
        self._closing = True

    def wait_close(self, timeout: float = -1):
        if timeout > 0:
            wait_until(timeout, 0.1, self._queue.empty)
            if not self._queue.empty():
                raise TimeoutException(timeout)


class Loop:
    def __init__(self):

        self._epoll = select.epoll()
        self.connections = {}
        self.requests = {}
        self.responses = {}

        self.server_fd = -1

        self._handlers = {}

    @classmethod
    def instance(cls):
        if not hasattr(cls, "_instance"):
            cls._instance = cls()
        return cls._instance

    def register_handler(self, fd):
        self._epoll.register(fd, select.EPOLLIN | select.EPOLLET)
        self.server_fd = fd

    def add_handler(self, fd, handler):
        self._handlers[fd] = handler

    def handle_connection(self, connection):
        self._epoll.register(connection.fileno(), select.EPOLLIN | select.EPOLLET)
        self.connections[connection.fileno()] = connection
        self.requests[connection.fileno()] = b""

    def start(self, event_to_wait, log, event_queue):

        while event_to_wait.is_set():

            events = self._epoll.poll(1)
            for fileno, event in events:

                if fileno == self.server_fd:
                    accept_connection = self._handlers[fileno]
                    accept_connection()

                elif event & select.EPOLLIN:
                    # try:
                    #    while True:
                    #        self.requests[fileno] += self.connections[fileno].recv(1024)
                    # except socket.error:
                    #    pass
                    # if EOL1 in self.requests[fileno] or EOL2 in self.requests[fileno]:
                    #    print(self.requests[fileno])
                    # self._epoll.modify(fileno, select.EPOLLIN | select.EPOLLET)
                    conn = self.connections[fileno]
                    data = json.loads(conn.recv(1024).decode())
                    log.info(f"Received message: {data}")
                    if "type" in data and data["type"] == "heartbeat":
                        conn.sendall(json.dumps({"status": "alive"}).encode())
                    elif "watch-event" in data:
                        event_queue.add_watch_notification(data)
                    else:
                        event_queue.add_indirect_result(data)

                # elif event & select.EPOLLOUT:
                #    try:
                #        while len(self.responses[fileno]) > 0:
                #            byteswritten = self.connections[fileno].send(self.responses[fileno])
                #            self.responses[fileno] = self.responses[fileno][byteswritten:]
                #    except socket.error:
                #        pass
                #    if len(self.responses[fileno]) == 0:
                #        self._epoll.modify(fileno, select.EPOLLET)
                #        self.connections[fileno].shutdown(socket.SHUT_RDWR)

                elif event & select.EPOLLHUP:
                    self._epoll.unregister(fileno)
                    self.connections[fileno].close()
                    del self.connections[fileno]

    def stop(self, fd):
        print("stopped epoll")
        self._epoll.unregister(fd)
        self._epoll.close()


class ResponseListener(Thread):
    """The thread receives replies and watch notifications from the service.
    After calling `run`, the thread runs in the background until `stop` is called.

    :param event_queue: reference to the event queue processing replies
    :param port: port to be used for listening for replies, defalts to -1
    """

    @property
    def address(self):
        return self._public_addr

    @property
    def port(self):
        return self._port

    def __init__(self, event_queue: EventQueue, port: int = -1):

        super().__init__(daemon=True)
        self._event_queue = event_queue
        self._work_event = Event()
        self._work_event.set()

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self._socket.bind(("", port if port != -1 else 0))
        self._socket.setblocking(False)

        req = urllib.request.urlopen("https://checkip.amazonaws.com")
        self._public_addr = req.read().decode().strip()
        self._port = self._socket.getsockname()[1]
        self._log = logging.getLogger("ResponseListener")

        self.loop = Loop.instance()

        self.start()

    def accept_connection(self):
        connection, address = self._socket.accept()
        self._log.info(f"Connected with {address}")
        connection.setblocking(0)
        self.loop.handle_connection(connection)

    def run(self):

        self._socket.listen(128)
        self.loop.register_handler(self._socket.fileno())
        self.loop.add_handler(self._socket.fileno(), self.accept_connection)
        self._socket.setblocking(0)
        self._log.info(f"Begin listening on {self._public_addr}:{self._port}")

        self.loop.start(self._work_event, self._log, self._event_queue)

        self._log.info(f"Close response listener thread on {self._public_addr}:{self._port}")
        self._socket.close()
        self._work_event.set()

    def stop(self):
        """
        Clear work event and wait until run method sets it again before exiting.
        This certifies that thread has finished.

        Since the thread listens on a socket with a time out of 0.5 seconds,
        the stopping can take up to 0.5 second in the worst case.
        """
        self._work_event.clear()
        self._work_event.wait()


# FIXME: this should be hidden in providers implementation
class SQSListener(Thread):
    def __init__(self, event_queue: EventQueue, cfg: Config):

        super().__init__(daemon=True)
        self._sqs = boto3.client("sqs", region_name=cfg.deployment_region)
        self._queue_name = f"faaskeeper-{cfg.deployment_name}-client-sqs"

        try:
            self._queue_url = self._sqs.get_queue_url(QueueName=self._queue_name)["QueueUrl"]
        except ClientError as error:
            logging.exception(f"Couldn't get queue named {self._queue_name}")
            raise error

        self._event_queue = event_queue
        self._work_event = Event()
        self._work_event.set()
        self._log = logging.getLogger("SQSListener")

        self.start()

    def run(self):

        self._log.info(f"Start SQS response listener thread")

        while self._work_event.is_set():

            response = self._sqs.receive_message(
                QueueUrl=self._queue_url,
                AttributeNames=["SentTimestamp"],
                MaxNumberOfMessages=10,
                MessageAttributeNames=["All"],
                WaitTimeSeconds=5,
            )
            if "Messages" not in response:
                continue

            receipt_handles = []

            for idx, msg in enumerate(response["Messages"]):
                data = json.loads(msg["Body"])
                self._log.info(f"Received message: {data}")
                if "type" in data and data["type"] == "heartbeat":
                    # FIXME: add heartbeats
                    pass
                elif "watch-event" in data:
                    self._event_queue.add_watch_notification(data)
                else:
                    self._event_queue.add_indirect_result(data)

                receipt_handles.append({"Id": str(idx), "ReceiptHandle": msg["ReceiptHandle"]})

                if len(receipt_handles) > 0:
                    self._sqs.delete_message_batch(QueueUrl=self._queue_url, Entries=receipt_handles)

        self._log.info(f"Close SQS response listener thread")
        self._work_event.set()

    def stop(self):
        """
        Clear work event and wait until run method sets it again before exiting.
        This certifies that thread has finished.
        """
        self._work_event.clear()
        self._work_event.wait()


class SubmitterThread(Thread):

    """
    The thread polls requests from work queue and submits them.
    After calling `run`, the thread runs in the background until `stop` is called.

    :param session_id: ID of active session
    :param service_name: name of FK deployment in cloud
    """

    def __init__(
        self,
        session_id: str,
        provider_client: ProviderClient,
        queue: WorkQueue,
        event_queue: EventQueue,
        response_handler: Union[ResponseListener, SQSListener],
    ):
        super().__init__(daemon=True)
        self._session_id = session_id
        self._queue = queue
        self._event_queue = event_queue
        self._provider_client = provider_client
        self._response_handler = response_handler
        self._log = logging.getLogger("WorkerThread")
        self._work_event = Event()
        self._work_event.set()

        self.start()

    def stop(self):
        """
        Sets stop event and wait until run method clears it.
        This certifies that thread has finished.
        """
        self._work_event.clear()
        self._work_event.wait()

    # FIXME: batching of write requests
    def run(self):

        self._log.info(f"Begin submission worker thread.")
        # FIXME: abstract it away
        if isinstance(self._response_handler, ResponseListener):
            listener_address = (self._response_handler.address, self._response_handler.port)
            listener_address_dict = {
                "sourceIP": self._response_handler.address,
                "sourcePort": self._response_handler.port,
            }
        else:
            listener_address = ()
            listener_address_dict = {}

        while self._work_event.is_set():

            submission = self._queue.get()
            if not submission:
                continue

            req_id, request, future = submission
            try:
                if request.is_cloud_request():
                    """
                    Send the request to execution to the underlying cloud service.
                    """
                    self._log.info(f"Begin executing operation: {request.name}")
                    self._event_queue.add_expected_result(req_id, request, future)
                    self._provider_client.send_request(
                        request_id=f"{self._session_id}-{req_id}",
                        data={**request.generate_request(), **listener_address_dict},
                    )
                else:
                    # FIXME launch on a pool - then it becomes expected result as well
                    try:
                        # FIXME: every operation should return (res, watch)
                        res = self._provider_client.execute_request(request, listener_address)
                        if res is not None and len(res) > 0:
                            if res[1]:
                                self._event_queue.add_watch(request.path, res[1])
                            self._event_queue.add_direct_result(req_id, res[0], future)
                        else:
                            self._event_queue.add_direct_result(req_id, res, future)
                    except Exception as e:
                        self._event_queue.add_direct_result(req_id, e, future)
            except ProviderException as e:
                self._event_queue.add_direct_result(req_id, e, future)
            except Exception as e:
                self._event_queue.add_direct_result(req_id, e, future)
                self._log.info(f"Finish executing operation: {request.name}")

        self._log.info(f"Close queue worker thread.")
        self._work_event.set()


class SorterThread(Thread):
    """
    The thread polls requests from the event queue,
    and sorts them while releasing results to the user.
    After calling `run`, the thread runs in the background until `stop` is called.

    :param session_id: ID of active session
    :param service_name: name of FK deployment in cloud
    """

    def __init__(self, queue: EventQueue):
        super().__init__(daemon=True)
        self._queue = queue
        self._log = logging.getLogger("SorterThread")
        self._work_event = Event()
        self._work_event.set()

        self.start()

    def stop(self):
        """
        Sets stop event and wait until run method clears it.
        This certifies that thread has finished.
        """
        self._work_event.clear()
        self._work_event.wait()

    def _check_timeout(self, futures: list):

        cur_timestamp = datetime.now().timestamp()
        i = 0
        while i < len(futures):
            fut = futures[i]
            fut_timestamp = fut[-1]
            # timeout!
            if cur_timestamp - fut_timestamp >= 5.0:
                fut[2].set_exception(TimeoutException(5.0))
                # remove the element from the list
                futures.pop(0)
                i += 1
            else:
                break

    def run(self):

        self._log.info(f"Begin sorter thread.")

        futures = []
        # results = []

        while self._work_event.is_set():

            processed_result = False
            submission = self._queue.get()

            # FIXME: add timestamps to find missing events
            # if not event.wait(5.0):
            if not submission:
                self._check_timeout(futures)
                continue

            # FIXME: watches should be handled in a different data structure
            # we received result
            if submission[0] == EventQueue.EventType.CLOUD_EXPECTED_RESULT:
                futures.append((*submission[1:], datetime.now().timestamp()))
            # we have a direct result
            elif submission[0] == EventQueue.EventType.CLOUD_DIRECT_RESULT:
                req_id, result, future = submission[1:]
                # FIXME - exists should always return node (fix implementation!)
                # FIXME - get_children should return the parent (fix implementation!)
                if result is not None and isinstance(result, Node):
                    timestamp = result.modified.system.sum

                    # assume get_watch is a query that does NOT remove watch from watches.
                    watches = self._queue.get_watches([hashlib.md5(result.path.encode()).hexdigest()], timestamp)
                    paths = []
                    for p in result.modified.epoch.version:
                        paths.append(p.split("_")[0])
                    other_watches = self._queue.get_watches(paths, timestamp)

                    if (len(watches) > 0 or len(other_watches) > 0):
                        self._queue.add_direct_result(req_id, result, future)
                        continue

                if isinstance(result, Exception):
                    future.set_exception(result)
                else:
                    future.set_result(result)
                processed_result = True
            elif submission[0] == EventQueue.EventType.CLOUD_INDIRECT_RESULT:

                result = submission[1]
                # event format is: {session_id}-{local_idx}
                req_id = int(result["event"].split("-")[1])

                if len(futures) == 0:
                    self._log.error(f"Ignoring the result {result} with ID {req_id} for a non-existing future")
                    continue

                # FIXME: enforce ordering
                assert futures[0][0] == req_id
                req_id, request, future, _ = futures.pop(0)
                request.process_result(result, future)
                processed_result = True
            elif submission[0] == EventQueue.EventType.WATCH_NOTIFICATION:

                # FIXME: ordering
                watch:Watch = submission[1]
                event = submission[2]

                watch.generate_message(event)

            # if we processed result, then timeout could not have happend
            if not processed_result:
                self._check_timeout(futures)

        self._log.info(f"Close queue worker thread.")
        self._work_event.set()
