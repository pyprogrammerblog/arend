from typing import Literal

from pydantic import (
    BaseModel,
    BaseSettings,
    RedisDsn,
    PostgresDsn,
    AmqpDsn,
)


class Settings(BaseSettings):

    # beanstalkd
    broker: Literal["redis", "beanstalk", "sqs"]
    broker_uri: str

    # backends
    backend: Literal["redis", "postgres", "ampqn", "beanstalk", "sqs"]
    backend_uri: str

    # general settings
    max_retries: int = 10
    backoff_factor: int = 1
    connect_timeout: int = 10  # seconds
    priority: int = None
    delay: int = None
    delay_factor: int = 10
    sleep_time_consumer: int = 1

    # redis settings
    redis_socket_timeout: int = 2 * 60
    redis_socket_connect_timeout: int = 2 * 60

    # mongo backend settings
    mongodb_max_pool_size: int = 10
    mongodb_min_pool_size: int = 0
