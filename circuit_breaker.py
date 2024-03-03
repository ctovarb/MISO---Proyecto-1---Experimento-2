import functools
import http
import logging
from datetime import datetime

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


class StateChoices:
    OPEN = "open"
    CLOSED = "closed"
    HALF_OPEN = "half_open"


class RemoteCallFailedException(Exception):
    pass


class CircuitBreaker:
    def __init__(self, func, exceptions, threshold, delay):
        self.func = func
        self.exceptions_to_catch = exceptions
        self.threshold = threshold
        self.delay = delay

        self.state = StateChoices.CLOSED
        self.last_attempt_timestamp = None
        self._failed_attempt_count = 0

    def update_last_attempt_timestamp(self):
        self.last_attempt_timestamp = datetime.utcnow().timestamp()

    def set_state(self, state):
        prev_state = self.state
        self.state = state
        logging.info(f"Changed state from {prev_state} to {self.state}")

    def handle_closed_state(self, *args, **kwargs):
        allowed_exceptions = self.exceptions_to_catch
        try:
            ret_val = self.func(*args, **kwargs)
            logging.info("Success: Remote call")
            self.update_last_attempt_timestamp()
            return ret_val
        except allowed_exceptions as e:
            logging.info("Failure: Remote call")
            self._failed_attempt_count += 1

            self.update_last_attempt_timestamp()

            if self._failed_attempt_count >= self.threshold:
                self.set_state(StateChoices.OPEN)
            raise RemoteCallFailedException from e

    def handle_open_state(self, *args, **kwargs):
        current_timestamp = datetime.utcnow().timestamp()
        if self.last_attempt_timestamp + self.delay >= current_timestamp:
            raise RemoteCallFailedException(f"Retry after {self.last_attempt_timestamp+self.delay-current_timestamp} secs")

        self.set_state(StateChoices.HALF_OPEN)
        allowed_exceptions = self.exceptions_to_catch
        try:
            ret_val = self.func(*args, **kwargs)
            self.set_state(StateChoices.CLOSED)
            self._failed_attempt_count = 0
            self.update_last_attempt_timestamp()
            return ret_val
        except allowed_exceptions as e:
            self._failed_attempt_count += 1

            self.update_last_attempt_timestamp()

            self.set_state(StateChoices.OPEN)

            raise RemoteCallFailedException from e

    def make_remote_call(self, *args, **kwargs):
        if self.state == StateChoices.CLOSED:
            return self.handle_closed_state(*args, **kwargs)
        if self.state == StateChoices.OPEN:
            return self.handle_open_state(*args, **kwargs)


class APICircuitBreaker:
    def __init__(self, exceptions=(Exception,), threshold=3, delay=15):
        self.obj = functools.partial(
            CircuitBreaker,
            exceptions=exceptions,
            threshold=threshold,
            delay=delay
        )

    def __call__(self, func):
        self.obj = self.obj(func=func)

        def decorator(*args, **kwargs):
            ret_val = self.obj.make_remote_call(*args, **kwargs)
            return ret_val

        return decorator

    def __getattr__(self, item):
        return getattr(self.obj, item)


circuit_breaker = APICircuitBreaker




