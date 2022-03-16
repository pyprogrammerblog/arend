from .mongo import MongoBackend
from .redis import RedisBackend
from .sql import SqlBackend
from arend.settings import settings
from functools import lru_cache


@lru_cache
def get_queue_backend(backend: str):
    backends = {
        "redis": RedisBackend,
        "mongo": MongoBackend,
        "sql": SqlBackend,
    }
    return backends.get(backend)


Backend = get_queue_backend(backend=settings.backend)
