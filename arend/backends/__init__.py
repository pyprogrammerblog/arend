from pydantic import BaseSettings
from typing import Union
from arend.backends.redis import RedisSettings, RedisTask
from arend.backends.mongo import MongoSettings, MongoTask
from arend.backends.sql import SQLSettings, SQLTask

__all__ = [
    "MongoSettings",
    "RedisSettings",
    "SQLSettings",
    "Settings",
    "RedisTask",
    "MongoTask",
    "SQLTask",
]


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

        AREND__SQL_DSN='postgresql+psycopg2://user:pass@postgres:5432/db'
        AREND__SQL_TABLE='logs'

    Redis::

        AREND__REDIS_HOST='redis'
        AREND__REDIS_DB='1'
        AREND__REDIS_PASSWORD='pass'

    Mongo::

        AREND__MONGO_CONNECTION='mongodb://user:pass@mongo:27017'
        AREND__MONGO_DB='db'
        AREND__MONGO_COLLECTION='logs'

    """

    arend: Union[RedisSettings, MongoSettings, SQLSettings]

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"

    def backend(self):
        """
        Return a Backend with configuration already set
        """
        return self.arend.backend()
