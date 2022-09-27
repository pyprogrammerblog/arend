from arend.settings import ArendSettings, Settings
from arend.backends import MongoTask, RedisTask
from typing import Callable, Union
from pydantic import BaseModel
from datetime import timedelta
from arend.settings.tasks import (
    TASK_DELAY,
    TASK_PRIORITY,
    TASK_RETRY_BACKOFF_FACTOR,
    TASK_MAX_RETRIES,
)

import functools
import logging

__all__ = ["arend_task", "ArendTask"]


logger = logging.getLogger(__name__)


class ArendTask(BaseModel):
    """
    ArendTask
    """

    name: str
    func: Callable
    queue: str = None
    priority: int = TASK_PRIORITY
    delay: Union[timedelta, int] = TASK_DELAY
    max_retries: int = TASK_MAX_RETRIES
    retry_backoff_factor: int = TASK_RETRY_BACKOFF_FACTOR
    settings: ArendSettings = None

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def __repr__(self):
        return f"<{self.__class__.__name__} at {self.__class__.__module__}>"

    def run(self, *args, **kwargs):
        """
        Run the task immediately.
        """
        return self.func(*args, **kwargs)

    def apply_async(
        self,
        queue: str = None,
        args: tuple = None,
        kwargs: dict = None,
        priority: int = TASK_PRIORITY,
        delay: Union[timedelta, int] = TASK_DELAY,
        max_retries: int = TASK_MAX_RETRIES,
        retry_backoff_factor: int = TASK_RETRY_BACKOFF_FACTOR,
        settings: ArendSettings = settings,
    ) -> Union[MongoTask, RedisTask]:
        """
        Run task asynchronously.
        """
        settings = settings or Settings().arend
        Task = settings.get_backend()

        # create and save a task in your backend
        task = Task(
            name=self.name,
            func=self.func,
            queue=self.queue or queue,
            args=args or Task.args,
            kwargs=kwargs or Task.kwargs,
            priority=priority
            or self.priority
            or settings.task_priority
            or TASK_PRIORITY,
            delay=delay or self.delay or settings.task_delay or TASK_DELAY,
            max_retries=max_retries
            or self.max_retries
            or settings.task_max_retries
            or TASK_MAX_RETRIES,
            retry_backoff_factor=retry_backoff_factor
            or self.retry_backoff_factor
            or settings.task_retry_backoff_factor
            or TASK_RETRY_BACKOFF_FACTOR,
        ).save()  # default to SCHEDULED

        task.send_to_queue()  # put into the queue and set task as PENDING

        return task  # return a PENDING task


def arend_task(
    queue: str = None,
    priority: int = TASK_PRIORITY,
    delay: Union[timedelta, int] = TASK_DELAY,
    max_retries: int = TASK_MAX_RETRIES,
    retry_backoff_factor: int = TASK_RETRY_BACKOFF_FACTOR,
    settings: ArendSettings = None,
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
                name=func.__name__,
                func=func,
                queue=queue,
                priority=priority,
                delay=delay,
                max_retries=max_retries,
                retry_backoff_factor=retry_backoff_factor,
                settings=settings,
            )

        return wrapper_register()

    return decorator
