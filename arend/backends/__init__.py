from .mongo import MongoBackend
from .redis import RedisBackend
from .sql import SqlBackend
from functools import lru_cache


@lru_cache
def get_backend(backend: str):
    backends = {
        "redis": RedisBackend,
        "mongo": MongoBackend,
        "sql": SqlBackend,
        # to be add more...
    }
    return backends.get(backend)
