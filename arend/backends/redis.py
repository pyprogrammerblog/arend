from arend.backends.base import TasksBackend
from arend.settings import settings

import datetime
import json
import logging
import redis


logger = logging.getLogger(__name__)


class _JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(obj)


class _JSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(
            self, object_hook=self.object_hook, *args, **kwargs
        )

    def object_hook(self, obj):
        ret = {}
        for key, value in obj.items():
            if (
                key in {"created", "updated", "start_time", "end_time"}
                and value
            ):
                ret[key] = datetime.datetime.fromisoformat(value)
            else:
                ret[key] = value
        return ret


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
            return json.loads(task, cls=_JSONDecoder)

    def update_one(self, uuid: str, update: dict):
        self.redis.set(uuid, json.dumps(update, cls=_JSONEncoder))

    def delete_one(self, uuid: str):
        task = self.redis.get(uuid)
        self.redis.delete(uuid)
        return 1 if task else 0
