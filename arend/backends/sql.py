import logging
from typing import Union
from datetime import datetime
from uuid import UUID, uuid4

from arend.backends.base import BaseBackend
from sqlmodel import Field, SQLModel

__all__ = ["SQLBackend"]


logger = logging.getLogger(__name__)


class SQLBackend(BaseBackend, SQLModel, table=True):
    """
    Mongo DB Adapter
    """
    uuid: UUID = Field(default_factory=uuid4, primary_key=True)

    @classmethod
    def get(cls, uuid: UUID):
        """
        Get object from DataBase
        """
        with SessionManager(engine) as session:
            return session.query(cls).get(uuid)

    def save(self):
        """
        Updates object in DataBase
        """
        self.updated = datetime.utcnow()
        with SessionManager(engine) as session:


        return self

    def delete(self) -> Union[int, None]:
        """
        Deletes object in DataBase
        """
        with SessionManager(engine) as session:
            if task := session.query(self).get(self.uuid):
                session.delete(task)
                session.commit()
                return task.uuid
