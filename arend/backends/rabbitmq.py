import logging
from functools import lru_cache
from uuid import UUID

from arend.backends.base import BaseBackend
from redis import Redis

logger = logging.getLogger(__name__)


__all__ = ["Task"]


class Task(BaseBackend):
    """
    Task (RedisBackend)
    """

    @lru_cache
    @classmethod
    def get_redis(cls) -> Redis:
        redis_client = Redis()
        return redis_client

    @classmethod
    def find_one(cls, uuid: UUID):
        task = cls.get_redis().hgetall(str(uuid))
        if task:
            return cls(**task)

    def save(self) -> "Task":
        """

        :return:
        """
        self.get_redis().hset(str(self.uuid), **self.dict())
        return self

    def delete(self):
        """

        :return:
        """
        self.get_redis().delete(str(self.uuid))
