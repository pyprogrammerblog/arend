from arend.settings.settings import Settings
from arend.backends import mongo, redis, sql
from functools import lru_cache


@lru_cache
def get_settings():
    return Settings()


settings = Settings()


@lru_cache
def get_backend(backend: str):
    backends = {
        "redis": redis.Task,
        "mongo": mongo.Task,
        "sql": sql.Task,
        # to be add more...
    }
    return backends.get(backend)
