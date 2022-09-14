from arend.settings import settings
from arend.utils.locking import Lock
from pydantic import BaseModel
from arend.backends.mongo import MongoSettings, MongoTask
from arend.backends.sql import SQLSettings, SQLTask
from arend.backends.redis import RedisSettings, RedisTask
from arend.backends import Settings
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
    queue: str = None
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
        queue: str = None,
        priority: int = None,
        delay: Union[timedelta, int] = 0,
        args: tuple = None,
        kwargs: dict = None,
        settings: Union[
            MongoSettings, RedisSettings, SQLSettings, None
        ] = None,
    ) -> Union[MongoTask, RedisTask, SQLTask]:
        """
        Run task asynchronously.
        """

        # get settings for your backend
        settings = settings or Settings()
        Task = settings.backend()

        # create and save a task in your backend
        task = Task(
            task_name=self.task_name,
            task_location=self.task_location,
            queue=self.queue or queue,
            args=args or Task.args,
            kwargs=kwargs or Task.kwargs,
            priority=priority or self.priority or Task.priority,
            delay=delay or self.delay or Task.delay,
            exclusive=self.exclusive,
        ).save()  # default to SCHEDULED

        # put into the queue and set task as PENDING
        task.send_to_queue()

        # return a PENDING task
        return task


def arend_task(
    queue: str = None,
    priority: int = None,
    delay: Union[timedelta, int] = None,
    exclusive: bool = False,
    settings: Union[MongoSettings, RedisSettings, SQLSettings, None] = None,
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
                settings=settings,
            )

        return wrapper_register()

    return decorator
