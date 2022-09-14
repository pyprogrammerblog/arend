import logging
import redis  # type: ignore
from typing import Dict, Type, List, Union
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field
from arend.backends.base import BaseTask
from contextlib import contextmanager

__all__ = ["RedisTask", "RedisSettings"]


logger = logging.getLogger(__name__)


class RedisTask(BaseTask):
    """
    RedisLog class. Defines the Log for Redis Backend

    Usage:

        >>> from arend.backends.redis import RedisSettings
        >>>
        >>> settings = RedisSettings(
        >>>     redis_host="redis",
        >>>     redis_port="6379",
        >>>     redis_db="logs",
        >>>     redis_password="pass"
        >>> )
        >>> RedisTask = RedisSettings.backend()  # type: Type[RedisLog]
        >>> task = RedisTask(name="My task", description="A cool task")
        >>> task.save()
        >>>
        >>> assert task.dict() == {"name": "My task", ...}
        >>> assert task.json() == '{"name": "My task", ...}'
        >>>
        >>> task = RedisTask.get(uuid=UUID("<your-uuid>"))
        >>> assert task.description == "A cool task"
        >>>
        >>> assert task.delete() == 1
    """

    class Meta:
        redis_host: str
        redis_port: int
        redis_db: int
        redis_password: str
        redis_extras: Dict

    @classmethod
    @contextmanager
    def redis_connection(cls):
        """
        Yield a redis connection
        """
        assert cls.Meta.redis_host, "Please set a redis host"
        assert cls.Meta.redis_port, "Please set a redis port"
        assert cls.Meta.redis_db, "Please set a redis db"
        assert cls.Meta.redis_password, "Please set a redis password"

        with redis.Redis(
            host=cls.Meta.redis_host,
            port=cls.Meta.redis_port,
            db=cls.Meta.redis_db,
            password=cls.Meta.redis_password,
            **cls.Meta.redis_extras,
        ) as r:
            yield r

    @classmethod
    def get(cls, uuid: UUID) -> Union["RedisTask", None]:
        """
        Get object from DataBase

        Usage:

            >>> ...
            >>> log = RedisTask.get(uuid=UUID("<your-uuid>"))
            >>> assert log.uuid == UUID("<your-uuid>")
            >>>
        """
        with cls.redis_connection() as r:  # type: redis.Redis
            if task := r.get(str(uuid)):
                return cls.parse_raw(task)
            return None

    def save(self) -> "RedisTask":
        """
        Updates/Creates object in DataBase

        Usage:

            >>> ...
            >>> task = RedisTask(name="My Task")
            >>> task.save()
            >>> task.description = "A new description"
            >>> task.save()
            >>> ...
        """
        self.updated = datetime.utcnow()
        with self.redis_connection() as r:  # type: redis.Redis
            r.set(str(self.uuid), self.json())
        return self

    def delete(self) -> int:
        """
        Deletes object in DataBase

        Usage:

            >>> ...
            >>> assert task.delete() == 1  # count deleted 1
            >>> assert task.delete() == 0  # count deleted 0
            >>> ...
        """
        with self.redis_connection() as r:  # type: redis.Redis
            return r.delete(str(self.uuid))


class RedisSettings(BaseModel):
    """
    Redis Settings. Defines settings for Redis Backend
    """

    redis_host: str = Field(..., description="Redis Host")
    redis_port: int = Field(default=6379, description="Redis Host")
    redis_db: int = Field(default=1, description="Redis DB")
    redis_password: str = Field(..., description="Redis Password")
    redis_extras: Dict = Field(
        default_factory=dict, description="Redis extras"
    )

    def backend(self) -> Type[RedisTask]:
        """
        Returns a RedisLog class and set Redis backend settings

        Usage:
            >>> from arend.backends.redis import RedisSettings
            >>>
            >>> settings = RedisSettings(
            >>>     redis_host="redis",
            >>>     redis_port="6379",
            >>>     redis_db="logs",
            >>>     redis_password="pass"
            >>> )
            >>> RedisTask = RedisSettings.backend()  # type: Type[RedisTask]
            >>> task = RedisTask(name="My task", description="A cool task")
            >>> task.save()
        """
        RedisTask.Meta.redis_host = self.redis_host
        RedisTask.Meta.redis_port = self.redis_port
        RedisTask.Meta.redis_db = self.redis_db
        RedisTask.Meta.redis_password = self.redis_password
        RedisTask.Meta.redis_extras = self.redis_extras
        return RedisTask


class RedisLogs(BaseModel):
    """
    Defines the RedisLogs collection
    """

    tasks: List[RedisTask] = Field(default_factory=list, description="Logs")
    count: int = Field(default=0, description="Count")
