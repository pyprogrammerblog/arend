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


def test_consumer(purge_queue):
    # test settings
    settings.reserve_timeout = 0
    settings.consumer_testing = True

    # broker - beanstalkd - backend - mongo
    settings.broker = "beanstalkd"
    settings.backend = "mongo"
    consumer(queue_name="queue")

    # broker - beanstalkd - backend - mongo
    settings.broker = "beanstalkd"
    settings.backend = "sql"
    consumer(queue_name="queue")

    # broker - beanstalkd - backend - mongo
    settings.broker = "beanstalkd"
    settings.backend = "redis"
    consumer(queue_name="queue")

    # broker - beanstalkd - backend - mongo
    settings.broker = "redis"
    settings.backend = "redis"
    consumer(queue_name="queue")
