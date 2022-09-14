import logging
from datetime import datetime
from uuid import UUID
from typing import Dict, Type, List, Union
from pydantic import BaseModel, Field
from pymongo.collection import Collection
from contextlib import contextmanager
from arend.backends.base import BaseTask
from pymongo.mongo_client import MongoClient

__all__ = ["MongoTask", "MongoSettings"]


logger = logging.getLogger(__name__)


class MongoTask(BaseTask):
    """
    MongoTask class. Defines the Task for Mongo Backend

    Usage:

        >>> from arend.backends.mongo import MongoSettings
        >>>
        >>> settings = MongoSettings(
        >>>     mongo_connection="mongodb://user:pass@mongo:27017",
        >>>     mongo_db="db",
        >>>     mongo_collection="Tasks"
        >>> )
        >>> MongoTask = MongoSettings.backend()  # type: Type[MongoTask]
        >>> task = MongoTask(name="My task", description="A cool task")
        >>> task.save()
        >>>
        >>> assert task.dict() == {"name": "My task", ...}
        >>> assert task.json() == '{"name": "My task", ...}'
        >>>
        >>> task = MongoTask.get(uuid=UUID("<your-uuid>"))
        >>> assert task.description == "A cool task"
        >>>
        >>> assert task.delete() == 1
    """

    class Meta:
        mongo_db: str
        mongo_connection: str
        mongo_collection: str

    @classmethod
    @contextmanager
    def mongo_collection(cls):
        """
        Yield a Mongo connection to our Tasks Collection
        """
        assert cls.Meta.mongo_connection, "Please set a db connection"
        assert cls.Meta.mongo_db, "Please set a db name"
        assert cls.Meta.mongo_collection, "Please set a db collection"

        with MongoClient(
            cls.Meta.mongo_connection, UuidRepresentation="standard"
        ) as client:  # type: MongoClient
            db = client.get_database(cls.Meta.mongo_db)
            collection = db.get_collection(cls.Meta.mongo_collection)
            yield collection

    @classmethod
    def get(cls, uuid: UUID) -> Union["MongoTask", None]:
        """
        Get object from DataBase

        Usage:

            >>> ...
            >>> task = MongoTask.get(uuid=UUID("<your-uuid>"))
            >>> assert task.uuid == UUID("<your-uuid>")
            >>>
        """
        with cls.mongo_collection() as collection:  # type: Collection
            if task := collection.find_one({"uuid": uuid}):
                return cls(**task)
            return None

    def save(self) -> "MongoTask":
        """
        Updates/Creates object in DataBase

        Usage:

            >>> ...
            >>> task = MongoTask(name="My Task")
            >>> task.save()
            >>> task.description = "A new description"
            >>> task.save()
            >>> ...
        """
        self.updated = datetime.utcnow()
        with self.mongo_collection() as collection:  # type: Collection
            collection.update_one(
                filter={"uuid": self.uuid},
                update={"$set": self.dict()},
                upsert=True,
            )
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
        with self.mongo_collection() as collection:  # type: Collection
            deleted = collection.delete_one({"uuid": self.uuid})
            return deleted.deleted_count


class MongoSettings(BaseModel):
    """
    Mongo Settings. Defines settings for Mongo Backend
    """

    mongo_connection: str = Field(..., description="Connection string")
    mongo_db: str = Field(..., description="Database name")
    mongo_collection: str = Field(..., description="Collection name")
    mongo_extras: Dict = Field(default_factory=dict, description="Extras")

    def backend(self) -> Type[MongoTask]:
        """
        Returns a MongoTask class and set Mongo backend settings

        Usage:

            >>> from arend.backends.mongo import MongoSettings
            >>>
            >>> settings = MongoSettings(
            >>>     mongo_connection="mongodb://user:pass@mongo:27017",
            >>>     mongo_db="db",
            >>>     mongo_collection="Tasks"
            >>> )
            >>> MongoTask = MongoSettings.backend()
            >>> task = MongoTask(name="My task")
            >>> task.save()
        """
        MongoTask.Meta.mongo_connection = self.mongo_connection
        MongoTask.Meta.mongo_db = self.mongo_db
        MongoTask.Meta.mongo_collection = self.mongo_collection
        return MongoTask


class MongoTasks(BaseModel):
    """
    Defines the MongoTasks collection
    """

    tasks: List[MongoTask] = Field(default_factory=list, description="Tasks")
    count: int = Field(default=0, description="Count")
