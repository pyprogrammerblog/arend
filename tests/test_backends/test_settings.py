from arend.backends import Settings
from arend.backends.sql import SQLSettings, SQLTask
from arend.backends.mongo import MongoSettings, MongoTask
from arend.backends.redis import RedisSettings, RedisTask


# passing params
def test_create_settings_passing_params_redis():

    redis_settings = RedisSettings(redis_password="pass", redis_host="redis")
    settings = Settings(arend=redis_settings)
    klass = settings.backend()
    assert issubclass(klass, RedisTask)
    assert klass.Meta.redis_password == RedisTask.Meta.redis_password == "pass"


def test_create_settings_passing_params_mongo():

    mongo_settings = MongoSettings(
        mongo_connection="mongodb://user:pass@mongo:27017",
        mongo_db="db",
        mongo_collection="logs",
    )
    settings = Settings(arend=mongo_settings)
    klass = settings.backend()
    assert issubclass(klass, MongoTask)
    assert (
        klass.Meta.mongo_connection
        == MongoTask.Meta.mongo_connection
        == "mongodb://user:pass@mongo:27017"
    )


def test_create_settings_passing_params_sql():

    sql_settings = SQLSettings(
        sql_dsn="postgresql+psycopg2://user:pass@postgres:5432/db",
        sql_table="logs",
    )
    settings = Settings(arend=sql_settings)
    klass = settings.backend()
    assert issubclass(klass, SQLTask)
    assert (
        klass.Meta.sql_dsn
        == SQLTask.Meta.sql_dsn
        == "postgresql+psycopg2://user:pass@postgres:5432/db"
    )


# env vars
def test_create_settings_env_vars_redis(env_vars_redis):
    settings = Settings()
    klass = settings.backend()
    assert klass == RedisTask
    assert klass.Meta.redis_password == RedisTask.Meta.redis_password == "pass"


def test_create_settings_env_vars_mongo(env_vars_mongo):
    settings = Settings()
    klass = settings.backend()
    assert klass == MongoTask
    assert (
        klass.Meta.mongo_connection
        == MongoTask.Meta.mongo_connection
        == "mongodb://user:pass@mongo:27017"
    )


def test_create_settings_env_vars_sql(env_vars_sql):
    settings = Settings()
    klass = settings.backend()
    assert klass == SQLTask
    assert (
        klass.Meta.sql_dsn
        == SQLTask.Meta.sql_dsn
        == "postgresql+psycopg2://user:pass@postgres:5432/db"
    )
