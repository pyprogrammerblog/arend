import logging
from datetime import timedelta
from typing import Callable, Union

from arend.backends import Task
from arend.settings import settings
from arend.utils.locking import Lock
from pydantic import BaseModel
from pystalkd.Beanstalkd import DEFAULT_PRIORITY

__all__ = ["AsyncTask"]


logger = logging.getLogger(__name__)


class AsyncTask(BaseModel):
    task_name: str
    task_location: str
    processor: Callable
    queue_name: str = None
    task_priority: int = None
    task_delay: Union[timedelta, int] = None
    is_exclusive: bool = False
    supress_exception: bool = False

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def __repr__(self):
        return f"<{self.__class__.__name__} at {self.task_location}>"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.supress_exception

    def run(self, *args, **kwargs):
        """
        Run the task immediately.
        """
        if self.is_exclusive:
            with Lock(name=f"{settings.env}.{self.task_location}"):
                return self.processor(*args, **kwargs)
        else:
            return self.processor(*args, **kwargs)

    def apply_async(
        self,
        queue_name: str = None,
        task_priority: str = None,
        task_delay: Union[timedelta, int] = 0,
        args: tuple = None,
        kwargs: dict = None,
    ) -> Task:
        """
        Run task asynchronously.
        """
        # settings
        queue_name = queue_name or self.queue_name or settings.queue_name
        task_delay = task_delay or self.task_delay or settings.task_delay or 0
        task_priority = (
            task_priority
            or self.task_priority
            or settings.task_priority
            or DEFAULT_PRIORITY
        )

        # insert in backend
        task = Task(
            task_name=self.task_name,
            task_location=self.task_location,
            queue_name=queue_name,
            task_priority=task_priority,
            task_delay=task_delay,
            args=args or (),
            kwargs=kwargs or {},
            exclusive=self.is_exclusive,
        ).save()  # set as PENDING (default)

        # broker send to queue
        task.send_to_queue()  # put into the queue and set as PENDING

        return task  # return a PENDING task
