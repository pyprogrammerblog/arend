from arend.backends.mongo import MongoSettings
from arend.backends.redis import RedisSettings
from arend.settings import ArendSettings, Settings
from tests.conftest import task_capitalize_all, task_capitalize
from arend.brokers.beanstalkd import BeanstalkdSettings
from arend.worker.consumer import consumer
import json


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
    task_capitalize.apply_async(
        queue="test", args=("capitalize",), settings=settings
    )
    assert 1 == mongo_backend.count_documents({})
    assert "PENDING" == mongo_backend.find_one({})["status"]

    consumer(queue="test", long_polling=False, timeout=0, settings=settings)

    assert 1 == mongo_backend.count_documents({})
    assert "FINISHED" == mongo_backend.find_one({})["status"]
    assert "Capitalize" == mongo_backend.find_one({})["result"]


def test_arend_task_redis_backend(beanstalkd_setting, redis_backend):

    settings = ArendSettings(
        beanstalkd=BeanstalkdSettings(host="beanstalkd", port=11300),
        backend=RedisSettings(redis_host="redis", redis_password="pass"),
    )

    assert 0 == redis_backend.dbsize()
    task = task_capitalize.apply_async(
        queue="test", args=("capitalize",), settings=settings
    )
    assert 1 == redis_backend.dbsize()

    assert "PENDING" == json.loads(redis_backend.get(str(task.uuid)))["status"]

    consumer(queue="test", long_polling=False, timeout=0, settings=settings)

    assert 1 == redis_backend.dbsize()
    result = json.loads(redis_backend.get(str(task.uuid)))
    assert "FINISHED" == result["status"]
    assert "Capitalize" == result["result"]


def test_arend_task_mongo_env_vars_backend(
    beanstalkd_setting, mongo_backend, env_vars_mongo
):

    settings = Settings().arend

    assert 0 == mongo_backend.count_documents({})
    task_capitalize_all.apply_async(
        queue="test", args=("capitalize",), settings=settings
    )
    assert 1 == mongo_backend.count_documents({})
    assert "PENDING" == mongo_backend.find_one({})["status"]

    consumer(queue="test", long_polling=False, timeout=0, settings=settings)

    assert 1 == mongo_backend.count_documents({})
    assert "FINISHED" == mongo_backend.find_one({})["status"]
    assert "CAPITALIZE" == mongo_backend.find_one({})["result"]


def test_arend_task_redis_env_vars_backend(
    beanstalkd_setting, redis_backend, env_vars_redis
):

    settings = Settings().arend

    assert 0 == redis_backend.dbsize()
    task = task_capitalize.apply_async(
        queue="test", args=("capitalize",), settings=settings
    )
    assert 1 == redis_backend.dbsize()

    assert "PENDING" == json.loads(redis_backend.get(str(task.uuid)))["status"]

    consumer(queue="test", long_polling=False, timeout=0, settings=settings)

    assert 1 == redis_backend.dbsize()
    result = json.loads(redis_backend.get(str(task.uuid)))
    assert "FINISHED" == result["status"]
    assert "Capitalize" == result["result"]
