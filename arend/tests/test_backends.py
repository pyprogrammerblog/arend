from arend.backends import get_backend
from arend.backends.mongo import MongoBackend
from arend.backends.redis import RedisBackend
from arend.backends.sql import SqlBackend
from arend.settings import settings
from pymongo import MongoClient


def test_select_backend():
    backend = get_backend(backend="redis")
    assert backend == RedisBackend

    backend = get_backend(backend="sql")
    assert backend == SqlBackend

    backend = get_backend(backend="mongo")
    assert backend == MongoBackend


def test_backend_mongo(task, delete_mongo_tasks):

    with MongoClient(settings.mongodb_string) as client:
        db = client[settings.mongodb_db]
        collection = db[settings.mongodb_db_tasks]

        assert 0 == collection.count_documents({})

        with MongoBackend() as backend:
            backend.update_one(uuid=task.uuid, update=task.dict())

        assert 1 == collection.count_documents({})

        with MongoBackend() as backend:
            task_dict = backend.find_one(uuid=task.uuid)
            assert task_dict["uuid"] == task.uuid
            assert task_dict["location"] == task.location
            assert task_dict["detail"] == task.detail

        with MongoBackend() as backend:
            assert backend.delete_one(uuid=str(task.uuid))

        assert 0 == collection.count_documents({})


def test_backend_redis(task, delete_redis_tasks):

    with RedisBackend() as backend:
        assert 0 == len(backend.redis.keys())

    with RedisBackend() as backend:
        backend.update_one(uuid=task.uuid, update=task.dict())
        assert 1 == len(backend.redis.keys())

    with RedisBackend() as backend:
        task_dict = backend.find_one(uuid=task.uuid)
        assert task_dict["uuid"] == task.uuid
        assert task_dict["location"] == task.location
        assert task_dict["detail"] == task.detail
        assert task_dict["created"] == task.created

    with RedisBackend() as backend:
        assert 1 == backend.delete_one(uuid=task.uuid)
        assert 0 == backend.delete_one(uuid=task.uuid)
        assert 0 == len(backend.redis.keys())


def test_backend_sql():
    pass
