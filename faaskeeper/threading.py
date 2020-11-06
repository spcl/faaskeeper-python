from threading import Event
from typing import Any


class Future:
    def __init__(self):
        self._result = None
        self._exception = None
        self._ready = False
        self._event = Event()
        self._callbacks = []

    def set_result(self, result: Any):
        self._result = result
        # FIXME: Callbacks
        self._event.set()

    def set_exception(self, exception: Exception):
        self._exception = exception
        # FIXME: Callbacks
        self._event.set()

    def get(self):
        self.wait()
        if self._exception:
            raise self._exception
        else:
            return self._result

    def wait(self) -> bool:
        if not self._ready:
            self._event.wait()
            self._event.set()
        return not self._exception

    def done(self) -> bool:
        return self._ready

    # FIXME:
    def add_callback(self):
        pass
