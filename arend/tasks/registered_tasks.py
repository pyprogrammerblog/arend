import importlib
import logging
from functools import lru_cache
from inspect import getmembers
from typing import Dict, List

from arend.tasks.async_task import AsyncTask

logger = logging.getLogger(__name__)


def is_async_task(object: object) -> bool:
    """Return true if the object is a AsyncTask."""

    return isinstance(object, AsyncTask)


@lru_cache
def registered_tasks(
    locations: List[str], file_name: str = "tasks"
) -> Dict[str, AsyncTask]:
    """

    :param locations:
    :param file_name:
    :return:
    """
    tasks = {}

    for location in locations:
        full_location = f"{location}.{file_name}"
        module = importlib.import_module(full_location)
        members = dict(getmembers(module, is_async_task))
        tasks.update({v.location: v for k, v in members.items()})

    return tasks
