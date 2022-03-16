from arend.backends.base import TasksBackend
from arend.settings import settings
from pymongo import MongoClient
from pymongo.collection import Collection

import logging


logger = logging.getLogger(__name__)


class MongoBackend(TasksBackend):
    def __init__(self):
        self.client: MongoClient = MongoClient(settings.mongodb_string)
        self.db = self.client[settings.mongodb_db]
        self.tasks: Collection = self.db[settings.mongodb_db_tasks]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def find_one(self, uuid: str):
        return self.tasks.find_one({"uuid": uuid}, {"_id": 0})

    def update_one(self, uuid: str, update: dict):
        self.tasks.update_one({"uuid": uuid}, {"$set": update}, upsert=True)

    def delete_one(self, uuid: str):
        result = self.tasks.delete_one({"uuid": uuid})
        return result.deleted_count
