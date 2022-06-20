import functools
import logging
from datetime import timedelta
from typing import Union

from arend.tasks.async_task import AsyncTask

__all__ = ["arend_task"]


logger = logging.getLogger(__name__)


def arend_task(
    queue_name: str = None,
    queue_priority: int = None,
    queue_delay: Union[timedelta, int] = None,
    exclusive: bool = False,
):
    """

    :param queue_name:
    :param queue_priority:
    :param queue_delay:
    :param exclusive:
    :return:


    Register functions as async functions
    Examples:

    @arend_task()
    def task(kwargs):
        return "a result"

    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper_register():
            return AsyncTask(
                task_name=func.__name__,
                task_location=f"{func.__module__}.{func.__name__}",
                processor=func,
                queue_name=queue_name,
                queue_priority=queue_priority,
                queue_delay=queue_delay,
                exclusive=exclusive,
            )

        return wrapper_register()

    return decorator
