from functools import lru_cache
from typing import Type

from arend.backends.base import BaseBackend
from arend.settings import settings
from arend.settings.mongo import MongoSettings
from arend.settings.rabbitmq import RabbitMQSettings
from arend.settings.redis import RedisSettings
from arend.settings.settings import ArendSettings
from arend.settings.sql import SQLSettings


@lru_cache
def get_backend(settings: ArendSettings) -> Type[BaseBackend]:
    if settings.backend.name == MongoSettings.name:
        from arend.backends.mongo import Task

    elif settings.backend.name == RedisSettings.name:
        from arend.backends.redis import Task

    elif settings.backend.name == SQLSettings.name:
        from arend.backends.sql import Task

    elif settings.backend.name == RabbitMQSettings.name:
        from arend.backends.mongo import Task

    else:
        raise ValueError("Not found")

    return Task


Task = get_backend(settings=settings)
