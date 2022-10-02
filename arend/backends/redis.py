import logging
from redis import Redis  # type: ignore
from typing import Dict, Type, List, Union, TYPE_CHECKING
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field
from arend.backends.base import BaseTask
from contextlib import contextmanager

if TYPE_CHECKING:
    from arend.settings import ArendSettings


__all__ = ["RedisTask", "RedisTasks", "RedisSettings"]


logger = logging.getLogger(__name__)


class RedisTask(BaseTask):
    """
    RedisLog class. Defines the Log for Redis Backend

    Usage:

        >>> from arend.backends.redis import RedisSettings
        >>>
        >>> settings = RedisSettings(
        >>>     redis_host="redis",
        >>>     redis_port=6379,
        >>>     redis_db="logs",
        >>>     redis_password="pass"
        >>> )
        >>> RedisTask = RedisSettings.get_backend()  # type: Type[RedisLog]
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
        settings: "ArendSettings"

    @classmethod
    @contextmanager
    def redis_connection(cls):
        """
        Yield a Redis connection to a specific database
        """
        host = cls.Meta.settings.backend.redis_host
        port = cls.Meta.settings.backend.redis_port
        db = cls.Meta.settings.backend.redis_db
        password = cls.Meta.settings.backend.redis_password

        with Redis(host=host, port=port, db=db, password=password) as r:
            yield r

    @classmethod
    def get(cls, uuid: UUID) -> Union["RedisTask", None]:
        """
        Get object from DataBase

        Usage:

            >>> log = RedisTask.get(uuid=UUID("<your-uuid>"))
            >>> assert log.uuid == UUID("<your-uuid>")
        """
        with cls.redis_connection() as r:  # type: Redis
            if task := r.get(str(uuid)):
                return cls.parse_raw(task)
            return None

    def save(self) -> "RedisTask":
        """
        Updates/Creates object in DataBase

        Usage:

            >>> task = RedisTask(name="My Task")
            >>> task.save()
            >>> task.description = "A new description"
            >>> task.save()
        """
        self.updated = datetime.utcnow()
        with self.redis_connection() as r:  # type: Redis
            r.set(str(self.uuid), self.json())
        return self

    def delete(self) -> int:
        """
        Deletes object in DataBase

        Usage:

            >>> assert task.delete() == 1  # count deleted 1
            >>> assert task.delete() == 0  # count deleted 0
        """
        with self.redis_connection() as r:  # type: Redis
            return r.delete(str(self.uuid))


class RedisTasks(BaseModel):
    """
    Defines the RedisTasks collection
    """

    tasks: List[RedisTask] = Field(default_factory=list, description="Logs")
    count: int = Field(default=0, description="Count")


class RedisSettings(BaseModel):
    """
    Defines settings for Redis Backend
    """

    redis_host: str = Field(..., description="Redis Host")
    redis_port: int = Field(default=6379, description="Redis Host")
    redis_db: int = Field(default=1, description="Redis DB")
    redis_password: str = Field(..., description="Redis Password")
    redis_extras: Dict = Field(
        default_factory=dict, description="Redis extras"
    )

    def get_backend(self) -> Type[RedisTask]:
        """
        Returns a RedisTask class with settings already set
        """
        return RedisTask
