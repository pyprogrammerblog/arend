from inspect import getmembers
from arend.arend import ArendTask
from typing import Dict
from typing import List
import importlib
import logging


logger = logging.getLogger(__name__)


def is_arend_task(object: object) -> bool:
    """Return true if the object is a ArendTask."""

    return isinstance(object, ArendTask)


def registered_tasks(
    locations: List[str], file_name: str = "tasks"
) -> Dict[str, ArendTask]:
    """"""
    tasks = {}

    for location in locations:
        full_location = f"{location}.{file_name}"
        module = importlib.import_module(full_location)
        members = dict(getmembers(module, is_arend_task))
        tasks.update({v.location: v for k, v in members.items()})

    return tasks
