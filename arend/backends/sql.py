import logging
from uuid import UUID
from uuid import uuid4
from typing import Dict, List, Union
from sqlmodel import Session, SQLModel
from sqlmodel import create_engine
from sqlmodel import select, Field
from pydantic import BaseModel
from datetime import datetime
from contextlib import contextmanager
from arend.backends.base import BaseTask


__all__ = ["SQLTask", "SQLSettings"]


logger = logging.getLogger(__name__)


class SQLTask(BaseTask, SQLModel, table=True):  # type: ignore
    """
    SQLTask class. Defines the Task for SQL Backend

    Usage:

        >>> from arend.backends.sql import SQLSettings
        >>>
        >>> settings = SQLSettings(
        >>>     sql_dsn="postgresql+psycopg2://user:pass@postgres:5432/db"
        >>> )
        >>> SQLTask = SQLSettings.backend()  # type: Type[SQLLog]
        >>> task = SQLTask(name="My task", description="A cool task")
        >>> task.save()
        >>>
        >>> assert task.dict() == {"name": "My task", ...}
        >>> assert task.json() == '{"name": "My task", ...}'
        >>>
        >>> task = SQLTask.get(uuid=UUID("<your-uuid>"))
        >>> assert task.description == "A cool task"
        >>>
        >>> assert task.delete() == 1
    """

    __tablename__ = "arend_tasks"

    uuid: UUID = Field(default_factory=uuid4, primary_key=True)

    class Meta:
        sql_dsn: str

    @classmethod
    @contextmanager
    def sql_session(cls):
        """
        Yield a connection
        """
        assert cls.Meta.sql_dsn, "Please set a sql dsn string"

        engine = create_engine(cls.Meta.sql_dsn)
        with Session(engine) as session:
            yield session

    @classmethod
    def get(cls, uuid: UUID) -> Union["SQLTask", None]:
        """
        Get object from DataBase

        Usage:

            >>> ...
            >>> task = SQLTask.get(uuid=UUID("<your-uuid>"))
            >>> assert task.uuid == UUID("<your-uuid>")
            >>>
        """
        with cls.sql_session() as session:  # type: Session
            statement = select(cls).where(cls.uuid == str(uuid))
            if task := session.exec(statement).first():
                return task
            return None

    def save(self) -> "SQLTask":
        """
        Updates/Creates object in DataBase

        Usage:

            >>> ...
            >>> task = SQLTask(name="My Task")
            >>> task.save()
            >>> task.description = "A new description"
            >>> task.save()
            >>> ...
        """
        self.updated = datetime.utcnow()
        with self.sql_session() as session:
            session.add(self)
            session.commit()
            session.refresh(self)
        return self

    def delete(self) -> int:
        """
        Deletes object in DataBase

        Usage:

            >>> ...
            >>> assert task.delete() == 1  # count deleted 1
            >>> assert task.delete() == 0  # count deleted 0
            >>> ...
        """
        with self.sql_session() as session:
            statement = select(SQLTask).where(self.uuid == self.uuid)
            if task := session.exec(statement).first():
                session.delete(task)
                session.commit()
                return 1
            return 0


class SQLTasks(BaseModel):
    """
    Defines the SQL Tasks collection
    """

    tasks: List[SQLTask] = Field(default_factory=list, description="Logs")
    count: int = Field(default=0, description="Count")


class SQLSettings(BaseModel):
    """
    SQL Settings. Returns a SQLLog class and set SQL backend settings

    Usage:

        >>> from arend.backends.sql import SQLSettings
        >>>
        >>> settings = SQLSettings(
        >>>     sql_dsn="postgresql+psycopg2://user:pass@postgres:5432/db"
        >>> )
        >>> SQLTask = SQLSettings.backend()  # type: Type[SQLLog]
        >>> task = SQLTask(name="My task", description="A cool task")
        >>> task.save()
    """

    sql_dsn: str = Field(..., description="SQLAlchemy dsn connection")
    sql_table: str = Field(default="arend_tasks", description="Table name")
    sql_extras: Dict = Field(default_factory=dict, description="Extras")

    def backend(self):
        SQLTask.Meta.sql_dsn = self.sql_dsn
        SQLTask.Meta.sql_extras = self.sql_extras
        SQLTask.__tablename__ = self.sql_table
        return SQLTask
