import redis
import pytest
import os
from arend.task import arend_task
from pymongo.mongo_client import MongoClient
from arend.consumer.consumer import consumer
from sqlalchemy_utils import drop_database
from sqlalchemy_utils import database_exists
from sqlalchemy_utils import create_database
from sqlmodel import create_engine, Session
from arend.backends.sql import SQLTask


# @pytest.fixture(scope="function")
# def lock_flush():
#     Lock("piet").flush()
#     yield
#     Lock("piet").flush()


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
def sql_backend():
    postgres_uri = "postgresql+psycopg2://user:pass@postgres:5432/db"
    if database_exists(postgres_uri):
        drop_database(postgres_uri)
    create_database(postgres_uri)

    engine = create_engine(url=postgres_uri)
    SQLTask.metadata.create_all(engine)

    with Session(engine) as session:
        yield session

    drop_database(postgres_uri)


@pytest.fixture(scope="function")
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


@arend_task(queue="test")
def task_capitalize(name: str):
    """
    Example task for testing
    """
    return name.capitalize()


@arend_task(queue="test")
def task_count(name: str, to_count: str):
    """
    Example task for testing
    """
    return name.count(x=to_count)
