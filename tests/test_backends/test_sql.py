import uuid
from arend.backends import Settings
from sqlmodel import select
from arend.backends.sql import SQLSettings, SQLTask


def test_create_settings_passing_params_sql(sql_backend):

    sql_settings = SQLSettings(
        sql_dsn="postgresql+psycopg2://user:pass@postgres:5432/db",
        sql_table="logs",
    )
    Task = sql_settings.backend()
    task: SQLTask = Task(task_name="My task")

    assert not sql_backend.exec(
        select(Task).where(Task.uuid == task.uuid)
    ).first()

    task.description = "A description"
    task = task.save()
    task = Task.get(uuid=task.uuid)

    assert task.description == "A description"
    assert isinstance(task.uuid, uuid.UUID)
    assert sql_backend.exec(select(Task).where(Task.uuid == task.uuid)).first()
    task.delete()
    assert not sql_backend.exec(
        select(Task).where(Task.uuid == task.uuid)
    ).first()


def test_create_settings_env_vars_sql(sql_backend, env_vars_sql):

    settings = Settings()
    Task = settings.backend()
    log: SQLTask = Task(task_name="My task")

    assert not sql_backend.exec(
        select(Task).where(Task.uuid == log.uuid)
    ).first()

    log.description = "A description"
    log = log.save()
    log = Task.get(uuid=log.uuid)

    assert log.description == "A description"
    assert isinstance(log.uuid, uuid.UUID)
    assert sql_backend.exec(select(Task).where(Task.uuid == log.uuid)).first()
    assert 1 == log.delete()
    assert not sql_backend.exec(
        select(Task).where(Task.uuid == log.uuid)
    ).first()
    assert 0 == log.delete()
