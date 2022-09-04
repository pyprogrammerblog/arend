from datetime import datetime

import pytest
from fastapi.testclient import TestClient
from pymongo.mongo_client import MongoClient
from arend.settings import settings
from arend.utils.locking import Lock
from arend.settings import status
from arend.api import arend_router
from arend.worker.consumer import consumer
from sqlalchemy_utils import drop_database, database_exists, create_database
from arend.backend.task import Task


@pytest.fixture(scope="function")
def lock_flush():
    Lock("piet").flush()
    yield
    Lock("piet").flush()


@pytest.fixture(scope="function")
def client():
    from fastapi import FastAPI

    app = FastAPI(debug=True)
    app.add_route(route=arend_router, path="api/")

    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def drop_mongo():
    """
    Clean mongo DB before and after
    """
    mongo_uri = ""
    with MongoClient(mongo_uri) as client:
        client.drop_database(settings.mongo_db)
        yield
        client.drop_database(settings.mongo_db)


@pytest.fixture
def drop_sql(sql_dsn):
    """
    Clean mongo DB before and after
    """
    sql_uri = ""
    if database_exists(url=sql_uri):
        drop_database(url=sql_uri)
    create_database(url=sql_uri)
    yield
    drop_database(url=sql_uri)


@pytest.fixture
def exhaust_queue():
    consumer(queue="test", timeout=0, long_polling=False)
    yield
    consumer(queue="test", timeout=0, long_polling=False)


@pytest.fixture
def tasks():
    """
    Tasks
    """
    task_1 = Task(
        status=status.FINISHED,
        start=datetime(year=2020, month=1, day=1, hour=1, minute=1),
        end=datetime(year=2020, month=1, day=1, hour=1, minute=30),
    ).save()
    task_2 = Task(
        status=status.FAIL,
        start=datetime(year=2020, month=1, day=3, hour=1, minute=1),
        end=datetime(year=2020, month=1, day=3, hour=1, minute=30),
    ).save()

    yield task_1, task_2

    task_1.delete()
    task_2.delete()
