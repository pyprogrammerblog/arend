from inspect import getmembers
from arend.task import ArendTask
from typing import Dict
from typing import List
from pathlib import Path

import importlib
import logging


logger = logging.getLogger(__name__)


def is_arend_task(obj: object) -> bool:
    """
    Return true if the object is a ArendTask.
    """
    return isinstance(obj, ArendTask)


def registered_tasks(
    locations: List[Path], file_name: str = "tasks"
) -> Dict[Path, ArendTask]:
    """
    Registered tasks
    """
    tasks = {}

    for location in locations:
        full_location = f"{str(location)}.{file_name}"
        module = importlib.import_module(full_location)
        members = dict(getmembers(module, is_arend_task))
        tasks.update({Path(v.task_location): v for k, v in members.items()})

    return tasks
