import redis
import pytest
import os
from arend import arend_task
from pymongo.mongo_client import MongoClient
from arend.settings.arend import BeanstalkdSettings
from arend.brokers.beanstalkd import BeanstalkdConnection


def flush_beanstalkd_queue(queue: str, settings: BeanstalkdSettings):
    with BeanstalkdConnection(queue=queue, settings=settings) as conn:
        while True:
            message = conn.reserve(timeout=0)
            if not message:
                break
            message.delete()


@pytest.fixture(scope="function")
def mongo_backend():
    with MongoClient(
        "mongodb://user:pass@mongo:27017", UuidRepresentation="standard"
    ) as client:
        client.drop_database("db")
        db = client.get_database("db")
        collection = db.get_collection("logs")
        yield collection
        client.drop_database("db")


@pytest.fixture(scope="function")
def redis_backend():
    with redis.Redis(host="redis", password="pass", port=6379, db=1) as r:
        r.flushdb()
        yield r
        r.flushdb()


@pytest.fixture(scope="function")
def beanstalkd_setting():
    settings = BeanstalkdSettings(host="beanstalkd", port=11300)
    flush_beanstalkd_queue(queue="test", settings=settings)
    yield settings
    flush_beanstalkd_queue(queue="test", settings=settings)


@pytest.fixture(scope="function")
def env_vars_redis():
    os.environ["AREND__BACKEND__REDIS_HOST"] = "redis"
    os.environ["AREND__BACKEND__REDIS_DB"] = "1"
    os.environ["AREND__BACKEND__REDIS_PASSWORD"] = "pass"
    os.environ["AREND__BEANSTALKD__HOST"] = "beanstalkd"
    os.environ["AREND__BEANSTALKD__PORT"] = "11300"
    try:
        yield
    finally:
        del os.environ["AREND__BACKEND__REDIS_HOST"]
        del os.environ["AREND__BACKEND__REDIS_DB"]
        del os.environ["AREND__BACKEND__REDIS_PASSWORD"]
        del os.environ["AREND__BEANSTALKD__HOST"]
        del os.environ["AREND__BEANSTALKD__PORT"]


@pytest.fixture(scope="function")
def env_vars_mongo():
    os.environ[
        "AREND__BACKEND__MONGO_CONNECTION"
    ] = "mongodb://user:pass@mongo:27017"
    os.environ["AREND__BACKEND__MONGO_DB"] = "db"
    os.environ["AREND__BACKEND__MONGO_COLLECTION"] = "logs"
    os.environ["AREND__BEANSTALKD__HOST"] = "beanstalkd"
    os.environ["AREND__BEANSTALKD__PORT"] = "11300"
    try:
        yield
    finally:
        del os.environ["AREND__BACKEND__MONGO_CONNECTION"]
        del os.environ["AREND__BACKEND__MONGO_DB"]
        del os.environ["AREND__BACKEND__MONGO_COLLECTION"]
        del os.environ["AREND__BEANSTALKD__HOST"]
        del os.environ["AREND__BEANSTALKD__PORT"]


@arend_task(queue="test")
def task_capitalize(name: str):
    """
    Example task for testing
    """
    return name.capitalize()


@arend_task(queue="test")
def task_capitalize_all(name: str):
    """
    Example task for testing
    """
    return name.upper()
