import uuid
from arend.settings import Settings, ArendSettings
from arend.settings.arend import BeanstalkdSettings
from arend.backends.redis import RedisTask, RedisSettings


def test_create_settings_passing_params_redis(redis_backend):

    redis_settings = RedisSettings(redis_host="redis", redis_password="pass")
    beanstalkd_settings = BeanstalkdSettings(host="beanstalkd", port=11300)
    settings = ArendSettings(
        backend=redis_settings, beanstalkd=beanstalkd_settings
    )

    Task = settings.get_backend()
    task: RedisTask = Task(name="My task", queue="test")

    assert not redis_backend.get(str(task.uuid))

    task.description = "A description"
    task = task.save()
    task = Task.get(uuid=task.uuid)

    assert task.description == "A description"
    assert isinstance(task.uuid, uuid.UUID)
    assert redis_backend.get(str(task.uuid))
    assert 1 == task.delete()
    assert not redis_backend.get(str(task.uuid))
    assert 0 == task.delete()


def test_create_settings_env_vars_redis(redis_backend, env_vars_redis):

    settings = Settings()
    Task = settings.arend.get_backend()
    task: RedisTask = Task(name="My task", queue="test")

    assert not redis_backend.get(str(task.uuid))

    task.description = "A description"
    task = task.save()
    task = Task.get(uuid=task.uuid)

    assert task.description == "A description"
    assert isinstance(task.uuid, uuid.UUID)
    assert redis_backend.get(str(task.uuid))
    assert 1 == task.delete()
    assert not redis_backend.get(str(task.uuid))
    assert 0 == task.delete()
