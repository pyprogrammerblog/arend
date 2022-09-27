from arend.backends.mongo import MongoSettings
from arend.worker.consumer import consumer
from arend.settings import ArendSettings, Settings
from tests.conftest import task_count, task_capitalize
from arend.brokers.beanstalkd import BeanstalkdSettings


def test_arend_task_mongo_backend(mongo_backend):

    assert 0 == mongo_backend.count_documents({})
    assert "To Capitalize" == task_capitalize(name="to capitalize")
    assert 0 == mongo_backend.count_documents({})

    task = task_capitalize.apply_async(queue="test", args=("to capitalize",))
    assert 1 == mongo_backend.count_documents({})
    assert "PENDING" == mongo_backend.find_one({})["status"]

    settings = ArendSettings(
        beanstalkd=BeanstalkdSettings(host="beanstalkd", port=11300),
        backend=MongoSettings(
            mongo_connection="mongodb://user:pass@mongo:27017",
            mongo_collection="logs",
            mongo_db="db",
        ),
    )
    consumer(queue="test", settings=settings)
    assert "SUCCESS" == mongo_backend.find_one({})["status"]

    Task = settings.get_backend()
    task = Task.get(uuid=task.uuid)
    assert task.result == "To Capitalize"
    assert task.start_time < task.end_time
    assert task.get_task_signature() == task_capitalize

    assert 1 == task.delete()
    assert 0 == mongo_backend.count_documents({})


def test_arend_task_redis_backend(redis_backend):
    pass


def test_arend_task_mongo_env_vars_backend(mongo_backend, env_vars_mongo):

    assert 0 == mongo_backend.count_documents({})
    assert "To Capitalize" == task_count(name="to capitalize", to_count="t")
    assert 0 == mongo_backend.count_documents({})

    task = task_count.apply_async(queue="test", args=("to capitalize", "t"))
    assert 1 == mongo_backend.count_documents({})
    assert "PENDING" == mongo_backend.find_one({})["status"]

    settings = Settings().arend

    consumer(queue="test", settings=settings)
    assert "SUCCESS" == mongo_backend.find_one({})["status"]

    Task = settings.get_backend()
    task = Task.get(uuid=task.uuid)
    assert task.result == "To Capitalize"
    assert task.start_time < task.end_time
    assert task.get_task_signature() == task_count

    assert 1 == task.delete()
    assert 0 == mongo_backend.count_documents({})


def test_arend_task_redis_env_vars_backend(redis_backend, env_vars_redis):
    pass
