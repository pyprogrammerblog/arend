from arend.settings import settings
from arend.worker.consumer import consumer
from arend.worker.processor import pool_processor

import pytest


def test_pool_processor():

    settings.reserve_timeout = 0
    settings.consumer_testing = True

    with pytest.raises(ValueError):
        pool_processor()

    settings.queues = {"queue": 1}

    pool_processor()


def test_consumer_broker_beanstalkd_backend_mongo(purge_queue):
    settings.reserve_timeout = 0
    settings.consumer_testing = True
    settings.broker = "beanstalkd"
    settings.backend = "mongo"

    consumer(queue_name="queue")
