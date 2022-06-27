import logging

import redis
from arend.settings import settings
from arend.utils.redis import get_redis

__all__ = ["Lock", "LockingException"]


logger = logging.getLogger(__name__)


class LockingException(Exception):
    pass


class Lock:
    def __init__(self, name: str, timeout: int = None):
        """
        Acquire a lock for an object
        """
        self.name = name
        self.lock = None
        self.timeout = timeout or 5 * 60  # 5 min
        self.client = get_redis(settings=settings)

    def flush(self):
        locks = list(self.client.scan_iter(self.name))
        if len(locks) > 0:
            self.client.delete(*locks)

    def acquire(self):
        lock = self.client.lock(self.name, timeout=self.timeout, sleep=0)
        if lock.acquire(blocking=False):
            self.lock = lock
            return lock
        else:
            raise LockingException(
                f"Could not lock '{self.name}' as it already has lock"
            )

    def release(self):
        if self.lock is not None:
            try:
                self.lock.release()
            except redis.exceptions.LockError:
                logger.warning(
                    f"Could not release lock '{self.lock.name}', "
                    f"maybe it has expired?"
                )
            self.lock = None

    def __enter__(self):
        return self.acquire()

    def __exit__(self, *args, **kwargs):
        self.release()
