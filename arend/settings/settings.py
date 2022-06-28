from typing import Dict, Union
from pydantic import BaseSettings
from arend.settings.backends.redis import RedisSettings
from arend.settings.backends.mongo import MongoSettings
from arend.settings.backends.sql import SQLSettings
from arend.settings.backends.rabbitmq import RabbitMQSettings
from arend.settings.broker.beanstalkd import BeanstalkdSettings


class ArendSettings(BaseSettings):

    # general task settings
    task_max_retries: int = 10
    task_priority: int = None
    task_delay: int = None
    task_delay_factor: int = 10

    # queues: key: name of the queue, value: concurrency
    queues: Dict[str, int] = None

    # backends
    backend: Union[MongoSettings, RedisSettings, SQLSettings, RabbitMQSettings]

    # broker
    broker: BeanstalkdSettings = BeanstalkdSettings()
