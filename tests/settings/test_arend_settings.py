import pytest
from arend.settings import ArendSettings
from arend.settings.backends.redis import RedisSettings
from arend.settings.backends.mongo import MongoSettings
from arend.settings.backends.sql import SQLSettings
from arend.settings.backends.rabbitmq import RabbitMQSettings


@pytest.mark.parametrize(
    "backend,klass_settings",
    [
        ("mongo", MongoSettings),
        ("sql", SQLSettings),
        ("rabbitmq", RabbitMQSettings),
        ("redis", RedisSettings),
    ],
)
def test_settings(backend, klass_settings):

    settings_dict = {"backend": {"backend_type": backend}}
    settings = ArendSettings(**settings_dict)
    assert settings.backend.backend_type == backend
    assert isinstance(settings.backend, klass_settings)
