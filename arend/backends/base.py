import abc
import logging
import traceback
from datetime import datetime
from typing import Any, Optional
from uuid import UUID, uuid4

from arend.settings import settings
from pydantic import BaseModel, Field

__all__ = ["BaseBackend", "Status"]


logger = logging.getLogger(__name__)


class Status:
    PENDING: str = "PENDING"
    STARTED: str = "STARTED"
    RETRY: str = "RETRY"
    FINISHED: str = "FINISHED"
    FAIL: str = "FAIL"


class BaseBackend(BaseModel, abc.ABC):
    """
    Base Task
    """

    uuid: UUID = Field(default_factory=uuid4, description="UID")
    status: str = Field(default=Status.PENDING, description="Current status")
    exclusive: bool = Field(default=False, description="")
    result: Optional[Any] = Field(default=None, description="Task result")
    detail: Optional[str] = Field(default=None, description="Task details")
    args: tuple = Field(default_factory=tuple, description="args arguments")
    kwargs: dict = Field(default_factory=dict, description="kwargs arguments")
    max_retries: int = Field(default=settings.max_retries, lte=10, gte=1)

    # beanstalkd settings
    queue_name: str = Field(default="default", description="Queue name")
    task_priority: int = Field(default=None, description="Queue priority")
    task_delay: int = Field(default=None, description="Queue delay")

    # meta task
    start_time: Optional[datetime] = Field(
        default=None, description="Datetime when task is STARTED"
    )
    end_time: Optional[datetime] = Field(
        default=None,
        description="Datetime when task is FINISHED, FAIL, or REVOKED",
    )
    task_name: str = Field(description="Full path task name")
    task_location: str = Field(description="Full path task location")
    created: datetime = Field(
        default_factory=datetime.utcnow, description="Created"
    )
    updated: datetime = Field(default=None, description="Updated")
    count_retries: int = Field(default=0, description="Count retries")
    supress_exception: bool = Field(
        default=True, description="Supress exception"
    )

    @abc.abstractmethod
    def save(self):
        pass

    @abc.abstractmethod
    def get(self, uuid: UUID):
        pass

    @abc.abstractmethod
    def delete(self):
        pass

    def notify(self, message: str):
        self.detail += f"- {message}\n"

    def __str__(self):
        return f"Task-{self.uuid}"

    def __enter__(self):
        self.start_time = datetime.utcnow()
        self.status = Status.STARTED
        self.save()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            last_trace = "".join(traceback.format_tb(exc_tb)).strip()
            self.detail = f"Failure: {last_trace}\n"
            if self.count_retries < self.max_retries or settings.max_retries:
                self.count_retries += 1
                self.status = Status.RETRY
            else:
                self.end_time = datetime.utcnow()
                self.status = Status.FAIL
            self.save()
            return self.supress_exception

    def __call__(self):
        return self.run()

    def run(self):
        """
        run task
        """

        with self:

            from arend.tasks.registered_tasks import registered_tasks

            # get signature
            registered = registered_tasks(
                locations=[settings.task_module_locations]
            )
            task = registered[self.task_location]

            # run task
            self.result = task.run(*self.args, **self.kwargs)

            # update finished task
            self.status = Status.FINISHED
            self.end_time = datetime.utcnow()
            self.save()
