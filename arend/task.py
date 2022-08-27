from arend.settings import settings
from arend.utils.locking import Lock
from arend.backend.task import Task
from pydantic import BaseModel
from pystalkd.Beanstalkd import DEFAULT_PRIORITY
from typing import Callable
from datetime import timedelta
from typing import Union

import functools
import logging

__all__ = ["arend_task", "ArendTask"]


logger = logging.getLogger(__name__)


class ArendTask(BaseModel):
    """
    ArendTask
    """

    task_name: str
    task_location: str
    processor: Callable
    queue_name: str = None
    priority: int = None
    delay: Union[timedelta, int] = None
    exclusive: bool = False

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def __repr__(self):
        return f"<{self.__class__.__name__} at {self.task_location}>"

    def run(self, *args, **kwargs):
        """
        Run the task immediately.
        """
        if self.exclusive:
            with Lock(name=f"{settings.env}.{self.task_location}"):
                return self.processor(*args, **kwargs)
        else:
            return self.processor(*args, **kwargs)

    def apply_async(
        self,
        queue_name: str = None,
        priority: str = None,
        delay: Union[timedelta, int] = 0,
        args: tuple = None,
        kwargs: dict = None,
    ) -> Task:
        """
        Run task asynchronously.
        """
        # settings
        queue_name = self.queue_name or queue_name or settings.queue
        delay = delay or self.delay or settings.delay or 0
        priority = (
            priority or self.priority or settings.priority or DEFAULT_PRIORITY
        )

        # insert in backend
        task = Task(
            task_name=self.task_name,
            task_location=self.task_location,
            args=args or (),
            kwargs=kwargs or {},
            queue_name=queue_name,
            priority=priority,
            delay=delay,
            exclusive=self.exclusive,
        ).save()  # Create a SCHEDULED (default) Object

        # put into the queue and set as PENDING
        task.send_to_queue()

        return task  # return a PENDING task


def arend_task(
    queue: str = None,
    priority: int = None,
    delay: Union[timedelta, int] = None,
    exclusive: bool = False,
):
    """
    Register functions as async functions
    Examples:

    @arend_task()
    def task(kwargs):
        return "a result"
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper_register():
            return ArendTask(
                task_name=func.__name__,
                task_location=f"{func.__module__}.{func.__name__}",
                processor=func,
                queue_name=queue,
                queue_priority=priority,
                queue_delay=delay,
                exclusive=exclusive,
            )

        return wrapper_register()

    return decorator
