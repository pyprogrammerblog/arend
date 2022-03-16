from arend.brokers.beanstalkd import BeanstalkdBroker
from arend.settings import settings
from arend.tube.task import Task
from pymongo import MongoClient

import pytest
import redis


@pytest.fixture(scope="session")
def task() -> Task:
    task = Task(
        name="name",
        location="location",
        queue_name="queue",
        priority=1,
        delay=1,
    )
    return task


@pytest.fixture(scope="function")
def delete_mongo_tasks():
    with MongoClient(settings.mongodb_string) as connection:
        db = connection[settings.mongodb_db]
        collection = db[settings.mongodb_db_tasks]
        collection.delete_many({})
        yield
        collection.delete_many({})


@pytest.fixture(scope="function")
def delete_redis_tasks():
    r = redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        password=settings.redis_password,
        socket_timeout=settings.socket_timeout,
        socket_connect_timeout=settings.socket_connect_timeout,
    )
    for key in r.keys():
        r.delete(key)
    yield
    for key in r.keys():
        r.delete(key)


def purge_beanstalkd_queue():
    settings.reserve_timeout = 0
    with BeanstalkdBroker(queue_name="test") as c:
        run = True
        while run:
            job = c.reserve()
            if job is None:
                run = False
            else:
                job.delete()


@pytest.fixture()
def purge_queue():
    purge_beanstalkd_queue()
    yield
    purge_beanstalkd_queue()


# @arend_task(queue_name="test")
# def sleep_task(seconds=0, return_value=None, event="success"):
#     """
#     For testing
#     """
#     print("executing sleep task...")
#     event = event.lower()
#     if event == "success":
#         time.sleep(int(seconds))
#     else:
#         raise ValueError(f"Unknown event '{event}'")
#
#     return return_value
