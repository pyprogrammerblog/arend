from arend.settings import settings
from pymongo import MongoClient
from pymongo.collection import Collection

import logging


logger = logging.getLogger(__name__)


class MongoTasksConnector:
    def __init__(self):
        self.db: MongoClient = MongoClient(settings.mongodb_string)
        collection = settings.mongodb_notifier_task_results
        self.tasks_collection: Collection = self.db[collection]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()

    def get_(self):
        pass
