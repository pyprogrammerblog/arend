from arend.backends.mongo import MongoSettings
from arend.backends.redis import RedisSettings
from arend.settings import ArendSettings, Settings
from tests.conftest import task_capitalize_all, task_capitalize
from arend.brokers.beanstalkd import BeanstalkdSettings


def test_arend_task_mongo_backend(beanstalkd_setting, mongo_backend):

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

    task.run()

    Task = settings.get_backend()
    task = Task.get(uuid=task.uuid)
    assert task.result == "Capitalize"
    assert task.start_time < task.end_time
    assert task.get_task_signature() == task_capitalize

    assert 1 == mongo_backend.count_documents({})
    assert 1 == task.delete()
    assert 0 == mongo_backend.count_documents({})


def test_arend_task_redis_backend(beanstalkd_setting, redis_backend):

    settings = ArendSettings(
        beanstalkd=BeanstalkdSettings(host="beanstalkd", port=11300),
        backend=RedisSettings(redis_host="redis", redis_password="pass"),
    )

    assert 0 == redis_backend.dbsize()
    assert "CAPITALIZE" == task_capitalize_all(name="capitalize")
    assert 0 == redis_backend.dbsize()

    task = task_capitalize_all.apply_async(
        queue="test", args=("capitalize",), settings=settings
    )
    assert 1 == redis_backend.dbsize()

    task.run()

    Task = settings.get_backend()
    task = Task.get(uuid=task.uuid)
    assert "FINISHED" == task.status
    assert task.result == "CAPITALIZE"
    assert task.start_time < task.end_time
    assert task.get_task_signature() == task_capitalize_all

    assert 1 == redis_backend.dbsize()
    assert 1 == task.delete()
    assert 0 == redis_backend.dbsize()


def test_arend_task_mongo_env_vars_backend(
    beanstalkd_setting, mongo_backend, env_vars_mongo
):

    settings = Settings().arend

    assert 0 == mongo_backend.count_documents({})
    assert "Capitalize" == task_capitalize(name="capitalize")
    assert 0 == mongo_backend.count_documents({})

    task = task_capitalize.apply_async(
        queue="test", args=("capitalize",), settings=settings
    )
    assert 1 == mongo_backend.count_documents({})
    assert "PENDING" == mongo_backend.find_one({})["status"]

    task.run()

    Task = settings.get_backend()
    task = Task.get(uuid=task.uuid)
    assert task.result == "Capitalize"
    assert task.start_time < task.end_time
    assert task.get_task_signature() == task_capitalize

    assert 1 == mongo_backend.count_documents({})
    assert 1 == task.delete()
    assert 0 == mongo_backend.count_documents({})


def test_arend_task_redis_env_vars_backend(
    beanstalkd_setting, redis_backend, env_vars_redis
):

    settings = Settings().arend

    assert 0 == redis_backend.dbsize()
    assert "CAPITALIZE" == task_capitalize_all(name="capitalize")
    assert 0 == redis_backend.dbsize()

    task = task_capitalize_all.apply_async(queue="test", args=("capitalize",))
    assert 1 == redis_backend.dbsize()

    task.run()

    Task = settings.get_backend()
    task = Task.get(uuid=task.uuid)
    assert "FINISHED" == task.status
    assert task.result == "CAPITALIZE"
    assert task.start_time < task.end_time
    assert task.get_task_signature() == task_capitalize_all

    assert 1 == redis_backend.dbsize()
    assert 1 == task.delete()
    assert 0 == redis_backend.dbsize()
