from pydantic import BaseSettings, RedisDsn
from typing import Literal


class RedisSettings(BaseSettings):

    backend_type: Literal["redis"] = "redis"
    host: str = "redis"
    port: int = 6379
    db: int = 1
    password: str = "password"
    socket_timeout: int = 2 * 60
    socket_connect_timeout: int = 2 * 60

    def get_connection_string(self):
        return
