import pytest
from arend.backends.base import BaseBackend, Status
from arend.backends.mongo import Task as MongoTask
from arend.backends.rabbitmq import Task as RabbitMQTask
from arend.backends.redis import Task as RedisTask
from arend.backends.sql import Task as SQLTask


@pytest.mark.parametrize(
    "task_class", [MongoTask, SQLTask, RabbitMQTask, RedisTask]
)
def test_task_create(task_class):
    task = task_class()
    task.save()

    retrieved = task_class.get(uuid=task.uuid)
    assert pytest.approx(task.dict(), retrieved.dict())

    assert task.task_name
    assert task.task_location

    assert isinstance(task, task_class)
    assert issubclass(task_class, BaseBackend)


@pytest.mark.parametrize(
    "task_class", [MongoTask, SQLTask, RabbitMQTask, RedisTask]
)
def test_task_update(task_class):
    task = task_class()
    task.save()

    retrieved = task_class.get(uuid=task.uuid)
    assert retrieved.status == Status.PENDING

    task.status = Status.FAIL
    task.save()  # updated

    retrieved = task_class.get(uuid=task.uuid)
    assert retrieved.status == Status.FAIL


@pytest.mark.parametrize(
    "task_class", [MongoTask, SQLTask, RabbitMQTask, RedisTask]
)
def test_task_delete(task_class):
    task = task_class()
    task.save()

    task.delete()  # delete
    assert not task_class.get(uuid=task.uuid)
