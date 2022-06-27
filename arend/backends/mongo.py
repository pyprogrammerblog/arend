import logging
from contextlib import contextmanager
from datetime import datetime
from uuid import UUID

from arend.backends.base import BaseBackend
from arend.settings import settings
from pymongo.collection import Collection
from pymongo.mongo_client import MongoClient

__all__ = ["Task"]


logger = logging.getLogger(__name__)


class Task(BaseBackend):
    """
    Mongo DB Adapter
    """

    @classmethod
    @contextmanager
    def mongo_collection(cls) -> Collection:
        """
        Yield a connection
        """
        db_conn = settings.mongo.connection
        db_name = settings.mongo.db
        db_collection = settings.mongo.collection

        with MongoClient(db_conn, UuidRepresentation="standard") as client:
            db = client.get_database(db_name)
            collection = db.get_collection(db_collection)
            yield collection

    @classmethod
    def get(cls, uuid: UUID) -> "Task":
        """
        Get
        :param uuid:
        :return:
        """
        with cls.mongo_collection() as collection:
            if obj := collection.find_one(filter={"uuid": uuid}):
                return cls(**obj)

    def save(self) -> "Task":
        """
        Save
        :return:
        """
        self.updated = datetime.utcnow()
        with self.mongo_collection() as collection:
            collection.update_one(
                filter={"uuid": self.uuid},
                update={"$set": self.dict()},
                upsert=True,
            )
        return self

    def delete(self) -> int:
        """
        Deletes object in DataBase
        """
        with self.mongo_collection() as collection:
            deleted = collection.delete_one({"uuid": self.uuid})
            return deleted.deleted_count
