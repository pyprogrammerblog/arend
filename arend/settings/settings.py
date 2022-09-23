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

    def get_backend(self) -> Type[Union[MongoTask, RedisTask, SQLTask]]:
        """
        Return a Task with configuration already set
        """
        Task = self.backend.get_backend()
        Task.Meta.settings = self
        return Task


class Settings(BaseSettings):

    arend: ArendSettings

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"
