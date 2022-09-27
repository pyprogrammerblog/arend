from tests.conftest import task_count
from arend.backends.mongo import MongoSettings
from arend.settings import ArendSettings, Settings
from arend.brokers.beanstalkd import BeanstalkdSettings


def test_arend_task_mongo_backend(mongo_backend):

    settings = ArendSettings(
        backend=MongoSettings(
            mongo_connection="mongodb://user:pass@mongo:27017",
            mongo_db="db",
            mongo_collection="logs",
        ),
        beanstalkd=BeanstalkdSettings(host="beanstalkd", port=11300),
    )
    Task = settings.get_backend()
    task = Task(name="My Task", queue="test", location="")

    assert 0 == mongo_backend.count_documents({})
    task.save()
    assert 1 == mongo_backend.count_documents({})

    task = Task.get(uuid=task.uuid)
    task.run()

    assert task.result == "SUCCESS"
    assert task.end_time is not None
    assert task.start_time < task.end_time

    task = Task.get(uuid=task.uuid)

    assert task.get_task_signature() == task_count
    assert 1 == task.delete()
    assert 0 == mongo_backend.count_documents({})


def test_arend_task_redis_backend(redis_backend):
    pass


def test_arend_task_mongo_env_vars_backend(mongo_backend, env_vars_mongo):
    settings = Settings().arend
    Task = settings.get_backend()
    task = Task(name="My Task", queue="test", location="")

    assert 0 == mongo_backend.count_documents({})
    task.save()
    assert 1 == mongo_backend.count_documents({})

    task = Task.get(uuid=task.uuid)
    task.run()

    assert task.result == "SUCCESS"
    assert task.end_time is not None
    assert task.start_time < task.end_time

    task = Task.get(uuid=task.uuid)

    assert task.get_task_signature() == task_count
    assert 1 == task.delete()
    assert 0 == mongo_backend.count_documents({})


def test_arend_task_redis_env_vars_backend(redis_backend, env_vars_redis):
    pass
