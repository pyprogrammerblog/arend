from arend.settings import Settings, ArendSettings
from arend.settings.arend import BeanstalkdSettings
from arend.backends.mongo import MongoSettings, MongoTask
from arend.backends.redis import RedisSettings, RedisTask


# passing params
def test_create_settings_passing_params_redis():

    redis_settings = RedisSettings(redis_password="pass", redis_host="redis")
    beanstalkd_settings = BeanstalkdSettings(host="beanstalkd", port=11300)
    settings = ArendSettings(
        backend=redis_settings, beanstalkd=beanstalkd_settings
    )

    klass = settings.get_backend()
    assert issubclass(klass, RedisTask)
    assert klass.Meta.settings == settings


def test_create_settings_passing_params_mongo():

    mongo_settings = MongoSettings(
        mongo_connection="mongodb://user:pass@mongo:27017",
        mongo_db="db",
        mongo_collection="logs",
    )
    beanstalkd_settings = BeanstalkdSettings(host="beanstalkd", port=11300)
    settings = ArendSettings(
        backend=mongo_settings, beanstalkd=beanstalkd_settings
    )

    klass = settings.get_backend()
    assert issubclass(klass, MongoTask)
    assert klass.Meta.settings == settings


# env vars
def test_create_settings_env_vars_redis(env_vars_redis):
    settings = Settings()
    klass = settings.arend.get_backend()
    assert klass.Meta.settings == settings.arend
    assert issubclass(klass, RedisTask)


def test_create_settings_env_vars_mongo(env_vars_mongo):
    settings = Settings()
    klass = settings.arend.get_backend()
    assert klass.Meta.settings == settings.arend
    assert issubclass(klass, MongoTask)
