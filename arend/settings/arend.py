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
    """
    Defines settings for the Arend

    Usage:
        >>> from arend import arend_task
        >>> from arend.backends.mongo import MongoSettings
        >>> from arend.brokers import BeanstalkdSettings
        >>> from arend.settings import ArendSettings
        >>> from arend.worker import consumer
        >>>
        >>> settings = ArendSettings(
        >>>     beanstalkd=BeanstalkdSettings(host="beanstalkd", port=11300),
        >>>     backend=MongoSettings(
        >>>         mongo_connection="mongodb://user:pass@mongo:27017",
        >>>         mongo_db="db",
        >>>         mongo_collection="Tasks"
        >>>     ),
        >>>     task_max_retries = 3
        >>>     task_retry_backoff_factor = 1
        >>>     task_priority = 0
        >>>     task_delay = 1
        >>> )
    """

    beanstalkd: BeanstalkdSettings
    backend: Union[MongoSettings, RedisSettings]
    task_max_retries: int = TASK_MAX_RETRIES
    task_retry_backoff_factor: int = TASK_RETRY_BACKOFF_FACTOR
    task_priority: int = TASK_PRIORITY
    task_delay: int = TASK_DELAY

    def get_backend(self) -> Type[Union[MongoTask, RedisTask]]:
        """
        Return a Task Backend with configuration already set
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
