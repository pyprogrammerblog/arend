import os

import pytest
from arend.settings import settings
from arend.utils.locking import Lock
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pymongo import MongoClient
from sqlmodel import SQLModel


@pytest.fixture(scope="session", autouse=True)
def override_environment_stage():
    os.environ["ENVIRONMENT"] = "DEVELOPMENT"
    yield


@pytest.fixture(scope="function")
def client():
    from arend.api.v1.router import arend_router

    app = FastAPI(title="Testing App")
    app.include_router(arend_router)  # add router for testing

    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture(scope="function")
def lock_flush():
    Lock("piet").flush()
    yield
    Lock("piet").flush()


@pytest.fixture(scope="session")
def engine():
    engine = settings.backend.sql.get_engine()
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def create_mysql_db(engine):
    if not database_exists(engine.url):
        create_database(engine.url)
    yield
    drop_database(engine.url)


@pytest.fixture(scope="function")
def sql(engine, create_mysql_db):
    SQLModel.metadata.drop_all(engine)
    SQLModel.metadata.create_all(engine)
    yield
    SQLModel.metadata.drop_all(engine)


@pytest.fixture(scope="function")
def mongo():
    """"""
    with MongoClient(settings.mongo_connection) as client:
        client.drop_database(settings.mongo_db)
        yield
        client.drop_database(settings.mongo_db)


@pytest.fixture(scope="function")
def redis():
    """"""
    redis = settings.backend.redis.get_redis()
    redis.flushdb()
    yield
    redis.flushdb()


@pytest.fixture
def rabbitmq(scope="function"):
    """"""
    pass
