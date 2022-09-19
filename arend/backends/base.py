from arend.broker import BeanstalkdBroker
from arend.backends import status
from datetime import datetime
from inspect import getmembers
from pydantic import BaseModel
from pydantic import Field
from typing import List
from typing import Optional
from uuid import uuid4, UUID
from typing import TYPE_CHECKING

import importlib
import logging
import traceback


if TYPE_CHECKING:
    from arend.task import ArendTask


logger = logging.getLogger(__name__)


class BaseTask(BaseModel):
    uuid: UUID = Field(default_factory=uuid4, description="UUID")
    name: str = Field(..., description="Full path task name")
    description: str = Field(default=None, description="Description")
    location: str = Field(default="tasks", description="Module location")

    status: str = Field(default=status.SCHEDULED, description="Status")
    result: Optional[str] = Field(default=None, description="Task result")
    detail: Optional[str] = Field(description="Task details")
    start_time: Optional[datetime] = Field(
        default=None, description="Task is STARTED"
    )
    end_time: Optional[datetime] = Field(
        default=None, description="Task is FINISHED, FAIL, or REVOKED"
    )

    args: tuple = Field(default_factory=tuple, description="Task args")
    kwargs: dict = Field(default_factory=dict, description="Task arguments")

    queue: str = Field(..., description="Queue name")
    delay: int = Field(default=0, description="Queue delay")
    priority: int = Field(default=1, description="Queue priority")

    created: datetime = Field(default_factory=datetime.utcnow)
    updated: datetime = None
    exclusive: bool = False
    count_retries: int = 0

    def save(self):
        return NotImplementedError

    def delete(self):
        return NotImplementedError

    @classmethod
    def get(cls, uuid: UUID):
        return NotImplementedError

    def send_to_queue(self):
        with BeanstalkdBroker(queue=self.queue) as broker:
            broker.put(
                body=str(self.uuid),
                priority=self.priority,
                delay=self.delay + self.count_retries * 1,
            )
            self.status = status.PENDING
            self.save()

    def notify(self, message: str):
        self.detail += f"- {message}\n"

    def __enter__(self):
        if self.status == [status.REVOKED, status.FAIL, status.FINISHED]:
            return

        self.start_time = datetime.utcnow()
        self.status = status.STARTED
        self.save()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            last_trace = "".join(traceback.format_tb(exc_tb)).strip()
            self.detail = f"Failure: {last_trace}\n"
            if self.count_retries < 3:
                self.count_retries += 1
                self.status = status.RETRY
                self.send_to_queue()  # put it in the tube again
            else:
                self.end_time = datetime.utcnow()
                self.status = status.FAIL
        else:
            self.status = status.FINISHED
            self.end_time = datetime.utcnow()
        self.save()
        return True

    def __call__(self):
        return self.run()

    def run(self):
        """
        Run task
        """
        with self:
            # get task and run it
            task = self.get_task_signature()
            result = task.run(*self.args, **self.kwargs)
            # update result
            self.result = result

    def is_arend_task(self, obj: ArendTask) -> bool:
        """ """
        return isinstance(obj, ArendTask)

    def get_task_signature(self) -> ArendTask:
        """ """
        module = importlib.import_module(self.location)
        members = dict(getmembers(module, self.is_arend_task))
        task = members[self.name]
        return task


class BaseTasks(BaseModel):
    tasks: List[BaseTask] = Field(default_factory=list)
    count: int = Field(default=0)
