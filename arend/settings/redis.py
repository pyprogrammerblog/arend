from pydantic import BaseModel, RedisDsn
from typing import Literal
from redis import Redis


class RedisSettings(BaseModel):

    name: Literal["redis"] = "redis"
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 1
    redis_password: str = "password"
    socket_timeout: int = 2 * 60
    socket_connect_timeout: int = 2 * 60
