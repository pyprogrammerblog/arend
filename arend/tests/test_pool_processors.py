from arend.settings import settings
from arend.worker.worker import pool_processor

import pytest


def test_pool_processor():
    settings.reserve_timeout = None
    settings.backend = "mongo"
    settings.broker = "redis"

    with pytest.raises(ValueError):
        pool_processor()

    settings.queues = {"queue": 1}

    pool_processor()
