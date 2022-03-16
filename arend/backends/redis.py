from arend.backends.base import TasksBackend
from arend.settings import settings

import json
import logging
import redis


logger = logging.getLogger(__name__)


class RedisBackend(TasksBackend):
    def __init__(self):
        self.conn = redis.Redis(
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
        self.conn.close()

    def find_one(self, uuid: str):
        task = self.conn.get(uuid)
        if task:
            return json.dumps(task)

    def update_one(self, uuid: str, update: dict):
        self.conn.set(uuid, json.dumps(update))

    def delete_one(self, uuid: str):
        self.conn.delete(uuid)
