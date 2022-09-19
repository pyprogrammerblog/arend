import logging
import redis  # type: ignore

logger = logging.getLogger(__name__)


__all__ = ["Lock", "LockingException"]


class LockingException(Exception):
    pass


class Lock:
    def __init__(self, name: str, timeout: int = None):
        """Acquire a lock for an object
        :param name: a string that uniquely determines the locked object
        :param timeout: the amount of seconds after a lock will expire
        """
        self.name = name
        self.timeout = timeout or 5 * 60  # 5 min
        self.lock = None
        self.client = redis.Redis(
            host="settings.redis_host",
            db=1,
            password="settings.redis_password",
        )

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
        self.client.close()
