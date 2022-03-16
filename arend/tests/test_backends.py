from arend.backends import get_queue_backend
from arend.backends.mongo import MongoBackend
from arend.backends.redis import RedisBackend
from arend.backends.sql import SqlBackend
from arend.settings import settings
from pymongo import MongoClient


def test_select_backend():
    backend = get_queue_backend(backend="redis")
    assert backend == RedisBackend

    backend = get_queue_backend(backend="sql")
    assert backend == SqlBackend

    backend = get_queue_backend(backend="mongo")
    assert backend == MongoBackend


def test_backend_mongo(task, delete_tasks):

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


def test_backend_redis():
    pass


def test_backend_sql():
    pass
