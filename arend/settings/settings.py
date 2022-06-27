from typing import Dict, Union
from pydantic import BaseSettings
from arend.settings.redis import RedisSettings
from arend.settings.mongo import MongoSettings
from arend.settings.sql import SQLSettings
from arend.settings.rabbitmq import RabbitMQSettings


class ArendSettings(BaseSettings):

    # general settings
    max_retries: int = 10
    backoff_factor: int = 1
    connect_timeout: int = 10  # seconds
    priority: int = None
    delay: int = None
    delay_factor: int = 10
    sleep_time_consumer: int = 1

    # queues: key: name of the queue, value: concurrency
    queues: Dict[str, int] = None
    reserve_timeout: int = 20

    # brokers and backends
    backend: Union[MongoSettings, RedisSettings, SQLSettings, RabbitMQSettings]

    beanstalkd_host: str = "beanstalkd"
    beanstalkd_port: int = 11300
