import uuid
from arend.backends import Settings
from arend.backends.mongo import MongoSettings, MongoTask


def test_create_settings_passing_params_mongo(mongo_backend):

    mongo_settings = MongoSettings(
        mongo_connection="mongodb://user:pass@mongo:27017",
        mongo_db="db",
        mongo_collection="logs",
    )
    Task = mongo_settings.backend()
    task: MongoTask = Task(task_name="My task")

    assert 0 == mongo_backend.count_documents(filter={})

    task.description = "A description"
    task = task.save()
    task = Task.get(uuid=task.uuid)

    assert task.description == "A description"
    assert isinstance(task.uuid, uuid.UUID)
    assert 1 == mongo_backend.count_documents(filter={})
    assert 1 == task.delete()
    assert 0 == mongo_backend.count_documents(filter={})
    assert 0 == task.delete()


def test_create_settings_env_vars_mongo(mongo_backend, env_vars_mongo):

    settings = Settings()
    MongoTask = settings.backend()
    task = MongoTask(task_name="My task")

    assert 0 == mongo_backend.count_documents(filter={})

    task.description = "A description"
    task = task.save()
    task = MongoTask.get(uuid=task.uuid)

    assert task.description == "A description"
    assert isinstance(task.uuid, uuid.UUID)
    assert 1 == mongo_backend.count_documents(filter={})
    assert 1 == task.delete()
    assert 0 == mongo_backend.count_documents(filter={})
    assert 0 == task.delete()
