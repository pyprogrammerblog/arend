from typing import Literal, Dict
from pydantic import BaseSettings


class Settings(BaseSettings):

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
    mongodb_string: str = "mongodb://mongo:mongo@mongo:27017"
    mongodb_max_pool_size: int = 10
    mongodb_min_pool_size: int = 0
    mongodb_db: str = "tasks"
    mongodb_db_tasks: str = "tasks"

    # queues: key: name of the queue, value: concurrency
    queues: Dict[str, int] = None
    reserve_timeout: int = 20

    # brokers and backends
    broker: Literal["redis", "beanstalk", "sqs"] = None
    backend: Literal["redis", "postgres", "mongo"] = None

    beanstalkd_host: str = None
    beanstalkd_port: int = None
