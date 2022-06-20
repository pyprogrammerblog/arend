import json
import logging

import redis
from arend.backends.base import BaseBackend, TasksBackend
from arend.settings import settings
from arend.utils.json import Decoder, Encoder

logger = logging.getLogger(__name__)


class RedisBackend(TasksBackend):
    def __init__(self):
        self.redis = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            password=settings.redis_password,
            socket_timeout=settings.socket_timeout,
            socket_connect_timeout=settings.socket_connect_timeout,
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.redis.close()

    def find_one(self, uuid: str):
        task = self.redis.get(uuid)
        if task:
            return json.loads(task, cls=Decoder)

    def update_one(self, uuid: str, update: dict):
        self.redis.set(uuid, json.dumps(update, cls=Encoder))

    def delete_one(self, uuid: str):
        task = self.redis.get(uuid)
        self.redis.delete(uuid)
        return 1 if task else 0
