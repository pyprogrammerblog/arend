import logging
from datetime import datetime
from typing import Union
from uuid import UUID, uuid4

from arend.backends.base import BaseBackend
from arend.settings import settings
from sqlmodel import Field, Session, SQLModel

__all__ = ["Task"]


logger = logging.getLogger(__name__)


class Task(BaseBackend, SQLModel, table=True):
    """
    Mongo DB Adapter
    """

    uuid: UUID = Field(default_factory=uuid4, primary_key=True)

    @classmethod
    def get(cls, uuid: UUID) -> Union["Task", None]:
        """
        Get object from DataBase
        """
        with Session(settings.sql.engine) as session:
            return session.query(cls).get(uuid)

    def save(self):
        """
        Updates object in DataBase
        """
        self.updated = datetime.utcnow()
        with Session(settings.sql.engine) as session:
            session.add(self)
            session.commit()
            session.refresh(self)

        return self

    def delete(self) -> Union[UUID, None]:
        """
        Deletes object in DataBase
        """
        with Session(settings.sql.engine) as session:
            if session.query(self).get(self.uuid):
                session.delete(self)
                session.commit()
                return self.uuid
