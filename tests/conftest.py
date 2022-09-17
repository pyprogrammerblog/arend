import redis
import pytest
import os
from pymongo.mongo_client import MongoClient
from arend.settings import settings
from arend.utils.locking import Lock
from arend.consumer.consumer import consumer
from sqlalchemy_utils import drop_database, database_exists, create_database


@pytest.fixture(scope="function")
def lock_flush():
    Lock("piet").flush()
    yield
    Lock("piet").flush()


@pytest.fixture
def mongo_backend():
    """
    Clean mongo DB before and after
    """
    mongo_uri = ""
    with MongoClient(mongo_uri) as client:
        client.drop_database(settings.mongo_db)
        yield
        client.drop_database(settings.mongo_db)


@pytest.fixture
def sql_backend():
    """
    Clean mongo DB before and after
    """
    sql_uri = ""
    if database_exists(url=sql_uri):
        drop_database(url=sql_uri)
    create_database(url=sql_uri)
    yield
    drop_database(url=sql_uri)


@pytest.fixture(scope="function")
def redis_backend():
    with redis.Redis(host="redis", password="pass", port=6379, db=1) as r:
        r.flushdb()
        yield r
        r.flushdb()


@pytest.fixture
def exhaust_queue():
    consumer(queue="test", timeout=0, long_polling=False)
    yield
    consumer(queue="test", timeout=0, long_polling=False)


@pytest.fixture(scope="function")
def env_vars_redis():
    os.environ["AREND__REDIS_HOST"] = "redis"
    os.environ["AREND__REDIS_DB"] = "1"
    os.environ["AREND__REDIS_PASSWORD"] = "pass"
    try:
        yield
    finally:
        del os.environ["AREND__REDIS_HOST"]
        del os.environ["AREND__REDIS_DB"]
        del os.environ["AREND__REDIS_PASSWORD"]


@pytest.fixture(scope="function")
def env_vars_mongo():
    os.environ["AREND__MONGO_CONNECTION"] = "mongodb://user:pass@mongo:27017"
    os.environ["AREND__MONGO_DB"] = "db"
    os.environ["AREND__MONGO_COLLECTION"] = "logs"
    try:
        yield
    finally:
        del os.environ["AREND__MONGO_CONNECTION"]
        del os.environ["AREND__MONGO_DB"]
        del os.environ["AREND__MONGO_COLLECTION"]


@pytest.fixture(scope="function")
def env_vars_sql():
    os.environ[
        "AREND__SQL_DSN"
    ] = "postgresql+psycopg2://user:pass@postgres:5432/db"
    os.environ["AREND__SQL_TABLE"] = "logs"
    try:
        yield
    finally:
        del os.environ["AREND__SQL_DSN"]
        del os.environ["AREND__SQL_TABLE"]
