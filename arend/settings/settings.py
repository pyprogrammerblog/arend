from pydantic import BaseSettings, BaseModel
from typing import Union, Type
from arend.brokers.beanstalkd import BeanstalkdSettings
from arend.backends.redis import RedisSettings, RedisTask
from arend.backends.mongo import MongoSettings, MongoTask
from arend.backends.sql import SQLSettings, SQLTask

__all__ = [
    "Settings",
    "ArendSettings",
    "MongoSettings",
    "RedisSettings",
    "SQLSettings",
    "RedisTask",
    "MongoTask",
    "SQLTask",
]


class ArendSettings(BaseModel):
    """ """

    beanstalkd: BeanstalkdSettings
    backend: Union[MongoSettings, RedisSettings, SQLSettings]
    task_max_retries: int = 10
    task_retry_backoff_factor: int = 1
    task_priority: int = None
    task_delay: int = None
    task_delay_factor: int = 10


class Settings(BaseSettings):
    """
    Different ways to pass settings to the `Settings` with priority order.

    **1. Passing settings** as parameters when creating a `Settings`
    object:

        >>> from arend.backends import MongoSettings, Settings
        >>>
        >>> mongo_settings = MongoSettings(
        >>>     mongo_connection="mongodb://user:pass@mongo:27017",
        >>>     mongo_db="db",
        >>>     mongo_collection="logs",
        >>> )
        >>> settings = Settings(arend=mongo_settings)

    **2. Environment variables**. Set you setting parameters in your
    environment. The `AREND__` prefix indicates that belongs to the
    `Arend` settings. The `Settings` will catch these settings.

    Examples:

    SQL::

        AREND__BACKEND__SQL_DSN='postgresql+psycopg2://user:pass@postgres:5432/db'
        AREND__BACKEND__SQL_TABLE='logs'

    Redis::

        AREND__BACKEND__REDIS_HOST='redis'
        AREND__BACKEND__REDIS_DB='1'
        AREND__BACKEND__REDIS_PASSWORD='pass'

    Mongo::

        AREND__BACKEND__MONGO_CONNECTION='mongodb://user:pass@mongo:27017'
        AREND__BACKEND__MONGO_DB='db'
        AREND__BACKEND__MONGO_COLLECTION='logs'

    """

    arend: ArendSettings

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"

    def backend(self) -> Type[Union[MongoTask, RedisTask, SQLTask]]:
        """
        Return a Task with configuration already set
        """
        Task = self.arend.backend.get_backend()
        Task.Meta.settings = self.arend
        return Task
