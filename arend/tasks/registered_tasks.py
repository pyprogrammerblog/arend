from inspect import getmembers
from notifier.arend.tasks.async_task import AsyncTask
from typing import Dict
from typing import List

import importlib
import logging


logger = logging.getLogger(__name__)


def is_async_task(object: object) -> bool:
    """Return true if the object is a AsyncTask."""

    return isinstance(object, AsyncTask)


def registered_tasks(
    locations: List[str], file_name: str = "tasks"
) -> Dict[str, AsyncTask]:
    """"""
    tasks = {}

    for location in locations:
        full_location = f"{location}.{file_name}"
        module = importlib.import_module(full_location)
        members = dict(getmembers(module, is_async_task))
        tasks.update({v.task_location: v for k, v in members.items()})

    return tasks
