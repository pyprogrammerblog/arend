from datetime import datetime
from inspect import getmembers
from pydantic import BaseModel
from pydantic import Field
from typing import List
from typing import Optional
from uuid import uuid4, UUID
from typing import TYPE_CHECKING
from arend.brokers.beanstalkd import BeanstalkdConnection

import importlib
import logging
import traceback


if TYPE_CHECKING:
    from arend.arend import ArendTask
    from arend.settings.settings import ArendSettings


logger = logging.getLogger(__name__)


class Status:
    SCHEDULED: str = "SCHEDULED"
    PENDING: str = "PENDING"
    STARTED: str = "STARTED"
    RETRY: str = "RETRY"
    FINISHED: str = "FINISHED"
    FAIL: str = "FAIL"
    REVOKED: str = "REVOKED"


class BaseTask(BaseModel):
    """
    Base Task
    """

    uuid: UUID = Field(default_factory=uuid4, description="UUID")
    name: str = Field(..., description="Full path task name")
    description: str = Field(default=None, description="Description")
    location: str = Field(default="tasks", description="Module location")

    status: str = Field(default=Status.SCHEDULED, description="Status")
    result: Optional[str] = Field(default=None, description="Task result")
    detail: Optional[str] = Field(description="Task details")
    start_time: Optional[datetime] = Field(default=None)
    end_time: Optional[datetime] = Field(default=None)

    args: tuple = Field(default_factory=tuple, description="Task args")
    kwargs: dict = Field(default_factory=dict, description="Task kwargs")

    queue: str = Field(..., description="Queue name")
    delay: int = Field(default=0, description="Queue delay")
    priority: int = Field(default=1, description="Queue priority")

    created: datetime = Field(default_factory=datetime.utcnow)
    updated: datetime = Field(default=None)
    count_retries: int = Field(default=0, description="Number of retries")
    max_retries: int = Field(default=3, description="Max retries")

    class Meta:
        settings: "ArendSettings"

    def save(self):
        return NotImplementedError

    def delete(self):
        return NotImplementedError

    @classmethod
    def get(cls, uuid: UUID):
        return NotImplementedError

    def send_to_queue(self):
        with BeanstalkdConnection(
            queue=self.queue, settings=self.Meta.settings.beanstalkd
        ) as conn:
            conn.put(
                body=str(self.uuid),
                priority=self.priority,
                delay=self.delay
                + self.count_retries
                * self.Meta.settings.task_retry_backoff_factor,
            )
            self.status = Status.PENDING
            self.save()

    def __enter__(self):
        if self.status == [Status.REVOKED, Status.FAIL, Status.FINISHED]:
            return

        self.start_time = datetime.utcnow()
        self.status = Status.STARTED
        self.save()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            last_trace = "".join(traceback.format_tb(exc_tb)).strip()
            self.detail = f"Failure: {last_trace}\n"
            if self.count_retries < self.max_retries:
                self.count_retries += 1
                self.status = Status.RETRY
                self.send_to_queue()  # put it in the tube again
            else:
                self.status = Status.FAIL
        else:
            self.status = Status.FINISHED

        self.end_time = datetime.utcnow()
        self.save()
        return True

    def __call__(self):
        return self.run()

    def run(self):
        """
        Run task.
        - Get signature
        - Run signature with args and kwargs
        - Update result
        """
        with self:
            task = self.get_task_signature()
            result = task.run(*self.args, **self.kwargs)
            self.result = result

    def get_task_signature(self) -> "ArendTask":
        """
        Get task signature.

        Returns: ArendTask. Returns an Arend Task signature.
        """
        module = importlib.import_module(self.location)
        tasks = dict(getmembers(module, lambda x: isinstance(x, ArendTask)))
        return tasks[self.name]


class BaseTasks(BaseModel):
    tasks: List[BaseTask] = Field(default_factory=list)
    count: int = Field(default=0)
