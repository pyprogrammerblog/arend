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
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 15
    redis_password: str = "password"
    socket_timeout: int = 2 * 60
    socket_connect_timeout: int = 2 * 60

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
    broker: Literal["redis", "beanstalkd", "sqs"] = "beanstalkd"
    backend: Literal["redis", "postgres", "mongo"] = "mongo"

    beanstalkd_host: str = "beanstalkd"
    beanstalkd_port: int = 11300

    # testing
    consumer_testing: bool = False
