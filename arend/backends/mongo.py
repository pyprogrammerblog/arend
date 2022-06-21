import logging
from contextlib import contextmanager
from datetime import datetime
from uuid import UUID, uuid4

from arend.backends.base import BaseBackend
from arend.settings import settings
from pymongo.collection import Collection
from pymongo.mongo_client import MongoClient

__all__ = ["MongoBackend"]


logger = logging.getLogger(__name__)


class MongoBackend(BaseBackend):
    """
    Mongo DB Adapter
    """

    class Meta:
        db_connection: str = settings.mongo_connection
        db_name: str = settings.mongo_db
        db_collection: str

    @classmethod
    @contextmanager
    def mongo_collection(cls) -> Collection:
        """
        Yield a connection
        """
        db_conn = (
            cls.Meta.db_connection
            if hasattr(cls.Meta, "db_connection")
            else MongoBackend.Meta.db_connection
        )
        db_name = (
            cls.Meta.db_name
            if hasattr(cls.Meta, "db_name")
            else MongoBackend.Meta.db_name
        )
        db_collection = cls.Meta.db_collection or "default"

        with MongoClient(db_conn, UuidRepresentation="standard") as client:
            db = client.get_database(db_name)
            collection = db.get_collection(db_collection)
            yield collection

    @classmethod
    def get(cls, uuid: UUID):
        """
        Get object from DataBase
        """
        with cls.mongo_collection() as collection:
            if obj := collection.find_one(filter={"uuid": uuid}):
                return cls(**obj)

    def save(self):
        """
        Updates object in DataBase
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
