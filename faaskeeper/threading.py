from threading import Condition
from typing import Any


class Future:
    def __init__(self):
        self._result = None
        self._exception = None
        self._ready = False
        self._event = Condition()
        self._callbacks = []

    def set_result(self, result: Any):
        self._result = result
        # FIXME: Callbacks
        self._event.notify_all()

    def set_exception(self, exception: Exception):
        self._exception = exception
        # FIXME: Callbacks
        self._event.notify_all()

    def get(self):
        self.wait()
        if self._exception:
            raise self._exception
        else:
            return self._result

    def wait(self) -> bool:
        if not self._ready:
            self._event.wait()
        return not self._exception

    def done(self) -> bool:
        return self._ready

    # FIXME:
    def add_callback(self):
        pass
