import functools
from typing import Union
from datetime import timedelta


def register(
    queue_name: str = None,
    queue_priority: str = None,
    delayed: Union[timedelta, int] = None
):
    """
    Register functions as SQS async functions
    Examples:
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
                delayed=delayed,
            )

        return wrapper_register()

    return decorator
