from .mongo import MongoBackend
from .redis import RedisBackend
from .sql import SqlBackend
from arend.settings import settings
from functools import lru_cache


@lru_cache
def get_queue_backend():
    backends = {
        "redis": RedisBackend,
        "mongo": MongoBackend,
        "sql": SqlBackend,
    }
    return backends.get(settings.broker)


QueueBroker = get_queue_backend()
