from pydantic import BaseSettings, BaseModel
from typing import Union, Type
from arend.brokers.beanstalkd import BeanstalkdSettings
from arend.backends.redis import RedisSettings, RedisTask
from arend.backends.mongo import MongoSettings, MongoTask
from arend.settings.tasks import (
    TASK_DELAY,
    TASK_PRIORITY,
    TASK_RETRY_BACKOFF_FACTOR,
    TASK_MAX_RETRIES,
)

import logging


logger = logging.getLogger(__name__)


__all__ = [
    "Settings",
    "ArendSettings",
    "MongoSettings",
    "RedisSettings",
    "RedisTask",
    "MongoTask",
]


class ArendSettings(BaseModel):
    """ """

    beanstalkd: BeanstalkdSettings
    backend: Union[MongoSettings, RedisSettings]
    task_max_retries: int = TASK_MAX_RETRIES
    task_retry_backoff_factor: int = TASK_RETRY_BACKOFF_FACTOR
    task_priority: int = TASK_PRIORITY
    task_delay: int = TASK_DELAY

    def get_backend(self) -> Type[Union[MongoTask, RedisTask]]:
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
