from arend.brokers.beanstalkd import BeanstalkdBroker
from arend.settings import settings
from arend.tube.task import Task
from pymongo import MongoClient

import pytest


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
def delete_tasks():
    with MongoClient(settings.mongodb_string) as connection:
        db = connection[settings.mongodb_db]
        collection = db[settings.mongodb_db_tasks]
        collection.delete_many({})
        yield
        collection.delete_many({})


def purge_beanstalkd_queue():
    settings.reserve_timeout = 0
    with BeanstalkdBroker(queue_name="test") as c:
        run = True
        while run:
            task_uuid = c.reserve()
            if task_uuid is None:
                run = False
            else:
                c.delete(task_uuid=task_uuid)


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
