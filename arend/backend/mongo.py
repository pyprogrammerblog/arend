from pymongo import MongoClient
from pymongo.collection import Collection
from arend.settings import base


class MongoTasksConnector:

    def __init__(self):
        self.db: MongoClient = MongoClient(base.mongodb_string)
        self.task_collection: Collection = self.db[base.mongodb_arend_task_results]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()
