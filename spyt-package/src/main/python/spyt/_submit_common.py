import time
from enum import Enum


class SubmissionStatus(Enum):
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    RELAUNCHING = "RELAUNCHING"
    UNKNOWN = "UNKNOWN"
    KILLED = "KILLED"
    FAILED = "FAILED"
    ERROR = "ERROR"
    UNDEFINED = "UNDEFINED"

    @classmethod
    def from_string(cls, name):
        try:
            return cls[name]
        except KeyError:
            raise StopIteration(name)

    @classmethod
    def is_final(cls, status):
        return status in _SUBMISSION_FINAL_STATES

    @classmethod
    def is_success(cls, status):
        return status is cls.FINISHED

    @classmethod
    def is_failure(cls, status):
        return status in (cls.UNKNOWN, cls.KILLED, cls.FAILED, cls.ERROR)


_SUBMISSION_FINAL_STATES = {
    SubmissionStatus.FINISHED, SubmissionStatus.UNKNOWN, SubmissionStatus.KILLED,
    SubmissionStatus.FAILED, SubmissionStatus.ERROR,
}


class ApplicationStatus(Enum):
    WAITING = "WAITING"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"
    KILLED = "KILLED"
    UNKNOWN = "UNKNOWN"
    UNDEFINED = "UNDEFINED"

    @classmethod
    def from_string(cls, name):
        try:
            return cls[name]
        except KeyError:
            raise StopIteration(name)

    @classmethod
    def is_final(cls, status):
        return status in (cls.FINISHED, cls.UNKNOWN, cls.KILLED, cls.FAILED)

    @classmethod
    def is_success(cls, status):
        return status is cls.FINISHED

    @classmethod
    def is_failure(cls, status):
        return status in (cls.UNKNOWN, cls.KILLED, cls.FAILED)

    @classmethod
    def is_waiting(cls, status):
        return status is cls.WAITING


def _retry_with_backoff(fn, max_attempts, base_delay_sec, max_delay_sec,
                        classify_transient, on_retry=None):
    delay = base_delay_sec
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as e:
            if not classify_transient(e):
                raise
            last_exc = e
            if attempt >= max_attempts:
                break
            if on_retry is not None:
                on_retry(attempt, e, delay)
            time.sleep(delay)
            delay = min(delay * 2, max_delay_sec)
    raise last_exc
