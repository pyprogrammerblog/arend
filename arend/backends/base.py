import abc
import logging
import traceback
from datetime import datetime
from typing import Any, List, Optional
from uuid import UUID, uuid4

from arend.settings import settings
from arend.utils.locking import Lock
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


__all__ = ["BaseBackend", "Status"]


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

    uuid: UUID = Field(default_factory=uuid4, description="UUID")
    description: str = Field(default=None, description="Description")
    status: str = Field(default=Status.PENDING, description="Status")
    result: Optional[Any] = Field(default=None, description="Task result")
    detail: str = Field(default="", description="Task details")
    max_retries: int = Field(default=settings.max_retries, lte=10, gte=1)
    count_retries: int = Field(default=0, description="Count retries")
    start_time: datetime = Field(default=None, description="Start time")
    end_time: datetime = Field(default=None, description="End time")
    created: datetime = Field(default_factory=datetime.utcnow)
    updated: datetime = Field(default=None, description="Updated")

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
            return True

    def __call__(self):
        return self.run()

    def run(self, threaded: bool = False):
        """
        Two CMs, first the Lock context and after the Task context.

        If the task is running by another Task object,
        raises an exception (not seen and cached by the Task context).

        # TODO: Re do this part...
        """

        with Lock(name=f"Task-{self.uuid}"), self:

            result = self.smart_stream.result(delayed=delayed)

            # update finished task
            self.status = Status.FINISHED
            self.end_time = datetime.utcnow()
            self.save()
