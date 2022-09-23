import uuid
from arend.settings import Settings, ArendSettings
from arend.settings.settings import BeanstalkdSettings
from arend.backends.mongo import MongoSettings
from tests.conftest import task_count


def test_create_settings_passing_params_mongo(mongo_backend):

    mongo_settings = MongoSettings(
        mongo_connection="mongodb://user:pass@mongo:27017",
        mongo_db="db",
        mongo_collection="logs",
    )
    beanstalkd_settings = BeanstalkdSettings(host="beanstalkd", port=11300)
    settings = ArendSettings(
        backend=mongo_settings, beanstalkd=beanstalkd_settings
    )

    Task = settings.get_backend()
    task = Task(name="My task", queue="test", func=task_count)

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
    Task = settings.arend.get_backend()
    task = Task(name="My task", queue="test", func=task_count)

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
