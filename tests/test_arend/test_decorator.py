from arend.backends.mongo import MongoSettings
from arend.worker.consumer import consumer
from arend.settings import ArendSettings, Settings
from tests.conftest import task_capitalize_all, task_capitalize
from arend.brokers.beanstalkd import BeanstalkdSettings


def test_arend_task_mongo_backend(flush_queue, mongo_backend):

    settings = ArendSettings(
        beanstalkd=BeanstalkdSettings(host="beanstalkd", port=11300),
        backend=MongoSettings(
            mongo_connection="mongodb://user:pass@mongo:27017",
            mongo_collection="logs",
            mongo_db="db",
        ),
    )

    assert 0 == mongo_backend.count_documents({})
    assert "Capitalize" == task_capitalize(name="capitalize")
    assert 0 == mongo_backend.count_documents({})

    task = task_capitalize.apply_async(
        queue="test", args=("capitalize",), settings=settings
    )
    assert 1 == mongo_backend.count_documents({})
    assert "PENDING" == mongo_backend.find_one({})["status"]

    consumer(queue="test", settings=settings, timeout=0)

    Task = settings.get_backend()
    task = Task.get(uuid=task.uuid)
    assert task.result == "Capitalize"
    assert task.start_time < task.end_time
    assert task.get_task_signature() == task_capitalize

    assert 1 == task.delete()
    assert 0 == mongo_backend.count_documents({})


def test_arend_task_redis_backend(redis_backend, flush_queue):

    settings = ArendSettings(
        beanstalkd=BeanstalkdSettings(host="beanstalkd", port=11300),
        backend=MongoSettings(
            mongo_connection="mongodb://user:pass@mongo:27017",
            mongo_collection="logs",
            mongo_db="db",
        ),
    )

    assert 0 == sum(redis_backend.keys())
    assert "CAPITALIZE" == task_capitalize_all(name="capitalize")
    assert 0 == sum(redis_backend.keys())

    task = task_capitalize_all.apply_async(
        queue="test", args=("capitalize",), settings=settings
    )
    assert 1 == sum(redis_backend.keys())
    assert "PENDING" == redis_backend.get(name=str(task.uuid))["status"]

    consumer(queue="test", settings=settings, timeout=0)

    Task = settings.get_backend()
    task = Task.get(uuid=task.uuid)
    assert task.result == "CAPITALIZE"
    assert task.start_time < task.end_time
    assert task.get_task_signature() == task_capitalize_all

    assert 1 == task.delete()
    assert 0 == sum(redis_backend.keys())


def test_arend_task_mongo_env_vars_backend(
    mongo_backend, env_vars_mongo, flush_queue
):

    settings = Settings().arend

    assert 0 == mongo_backend.count_documents({})
    assert "CAPITALIZE" == task_capitalize_all(name="capitalize")
    assert 0 == mongo_backend.count_documents({})

    task = task_capitalize_all.apply_async(queue="test", args=("capitalize",))
    assert 1 == mongo_backend.count_documents({})
    assert "PENDING" == mongo_backend.find_one({})["status"]

    consumer(queue="test", settings=settings, timeout=0)

    Task = settings.get_backend()
    task = Task.get(uuid=task.uuid)
    assert task.result == "CAPITALIZE"
    assert task.start_time < task.end_time
    assert task.get_task_signature() == task_capitalize_all

    assert 1 == task.delete()
    assert 0 == mongo_backend.count_documents({})


def test_arend_task_redis_env_vars_backend(
    redis_backend, env_vars_redis, flush_queue
):

    settings = Settings().arend

    assert 0 == sum(redis_backend.keys())
    assert "CAPITALIZE" == task_capitalize_all(name="capitalize")
    assert 0 == sum(redis_backend.keys())

    task = task_capitalize_all.apply_async(queue="test", args=("capitalize",))
    assert 1 == sum(redis_backend.keys())
    assert "PENDING" == redis_backend.get(name=str(task.uuid))["status"]

    consumer(queue="test", settings=settings, timeout=0)

    Task = settings.get_backend()
    task = Task.get(uuid=task.uuid)
    assert task.result == "CAPITALIZE"
    assert task.start_time < task.end_time
    assert task.get_task_signature() == task_capitalize_all

    assert 1 == task.delete()
    assert 0 == sum(redis_backend.keys())
