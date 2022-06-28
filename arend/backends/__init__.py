from functools import lru_cache
from typing import Type, Union

from arend.backends.base import BaseBackend
from arend.settings import settings
from arend.settings.backends.mongo import MongoSettings
from arend.settings.backends.rabbitmq import RabbitMQSettings
from arend.settings.backends.redis import RedisSettings
from arend.settings.backends.sql import SQLSettings
from arend.settings.settings import ArendSettings

Backends = Union[MongoSettings, RedisSettings, SQLSettings, RabbitMQSettings]


@lru_cache
def get_backend(settings: ArendSettings) -> Type[BaseBackend]:
    if settings.backend.name == MongoSettings.backend_type:
        from arend.backends.mongo import Task

    elif settings.backend.name == RedisSettings.backend_type:
        from arend.backends.redis import Task

    elif settings.backend.name == SQLSettings.backend_type:
        from arend.backends.sql import Task

    elif settings.backend.name == RabbitMQSettings.name:
        from arend.backends.mongo import Task

    else:
        raise ValueError("Not found")

    return Task


Task = get_backend(settings=settings)
