
import logging
import socket
import urllib
from threading import Thread


class ResponseHandler:

    @property
    def address(self):
        return self._public_addr

    @property
    def port(self):
        return self._port

    def __init__(self, port: int = -1):

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind(('', port if port != -1 else 0))

        req = urllib.request.urlopen('https://checkip.amazonaws.com')
        self._public_addr = req.read().decode().strip()
        self._port = self._socket.getsockname()[1]

    def start(self):
        self._thread = Thread(target=self._listen)
        self._thread.start()

    def stop(self):
        self._thread.join()

    def _listen(self):

        self._socket.listen(5)

        with open('log', 'w') as f:
            while True:

                conn, addr = self._socket.accept()
                print(conn, file=f)
                print(addr, file=f)
                f.flush()
                with conn:
                    logging.info(f"Connected with {addr}")
                    data = conn.recv(1024)
                    print(data)
                    print(data, file=f)
                    f.flush()
                    break

