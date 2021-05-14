import importlib
from inspect import getmembers
from typing import List
from pydantic import BaseModel
from typing import Any, Optional, Callable, Dict
from pydantic import Field
from datetime import datetime
from bson import ObjectId
from arend.backend.mongo import MongoTasksConnector
from arend.async_task import AsyncTask
from arend.queue.updater import ProgressUpdater
from arend.settings import base


def isasynctask(object):
    """Return true if the object is a AsyncTask."""

    return isinstance(object, AsyncTask)


def registered_tasks(locations: List[str], file_name: str = "tasks") -> Dict[str, Callable]:
    """

    :param file_name:
    :param location: str. 'sqs_async.test_project'
    :param file: str. 'tasks'
    :return: dict
    """
    tasks = {}

    for location in locations:
        full_location = f"{location}.{file_name}"
        module = importlib.import_module(full_location)
        members = dict(getmembers(module, isasynctask))
        tasks.update({f"{full_location}.{k}": v for k, v in members.items()})

    return tasks


class QueueTask(BaseModel):
    id: str = Field(..., alias="_id", description="Result ID")
    task_name: str = Field(..., description="Full path task name.")
    status: base.STATUSES = Field(description="Current task status.", default="PENDING")
    result: Optional[Any] = Field(description="Task result.")
    detail: Optional[Any] = Field(description="Task details.")
    parent_id: Optional[str] = Field(description="Parent task ID.")
    date_done: Optional[datetime] = Field(description="Task is either finished, fail, or revoked.")
    args: tuple = Field(default=(), description="Args arguments.")
    kwargs: dict = Field(default={}, description="Kwargs arguments.")

    def save(self):
        with MongoTasksConnector() as conn:
            conn.task_collection.find_one_and_update(
                filter={"_id": ObjectId(self.id)},
                update={"$set": self.dict()},
                upsert=True
            )

    def run(self, verbose=False):
        with ProgressUpdater(queue_task=self, verbose=verbose):
            tasks = base.registered_tasks
            return tasks[self.task_name](*self.args, **self.kwargs)

    def __call__(self, verbose=False):
        return self.run(verbose=verbose)

    @classmethod
    def get(cls, id: str):
        with MongoTasksConnector() as tasks:
            f = {"_id": ObjectId(id)}
            queue_task = tasks.task_collection.find_one(filter=f)

        if queue_task:
            return QueueTask(**queue_task)
