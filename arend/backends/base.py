from arend.broker import BeanstalkdBroker
from arend.settings import settings
from arend.backends import status
from datetime import datetime
from arend.utils.locking import Lock
from pydantic import BaseModel, FilePath
from pydantic import Field
from typing import Any
from typing import Optional
from uuid import uuid4, UUID
from typing import List

import logging
import traceback


logger = logging.getLogger(__name__)


DEFAULT_TTR = 30 * 60  # 30 min


class BaseTask(BaseModel):
    uuid: UUID = Field(default_factory=uuid4, description="UUID")
    name: str = Field(description="Full path task name")
    description: str = Field(default=None, description="Description")
    location: FilePath = Field(description="Full path task location")

    status: str = Field(default=status.SCHEDULED, description="Status")
    result: Optional[Any] = Field(default=None, description="Task result")
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
                delay=self.delay + self.count_retries * settings.delay_factor,
            )
            self.status = status.PENDING
            self.save()

    def notify(self, message: str):
        self.detail += f"- {message}\n"

    def __enter__(self):
        self.start_time = datetime.utcnow()
        self.status = status.STARTED
        self.save()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            last_trace = "".join(traceback.format_tb(exc_tb)).strip()
            self.detail = f"Failure: {last_trace}\n"
            if self.count_retries < settings.task_max_retries:
                self.count_retries += 1
                self.status = status.RETRY
                self.save()
                self.send_to_queue()  # put in the tube again
            else:
                self.end_time = datetime.utcnow()
                self.status = status.FAIL
            self.save()
            return True

    def __call__(self):
        return self.run()

    def run(self):

        with self, Lock(str(self.uuid)):
            if self.status == [status.REVOKED, status.FAIL, status.FINISHED]:
                return

            from arend.utils.registered_tasks import registered_tasks

            registered = registered_tasks(locations=settings.task_locations)
            task = registered[self.location]

            result = task.run(*self.args, **self.kwargs)

            # update finished task
            self.status = status.FINISHED
            self.end_time = datetime.utcnow()
            self.result = result
            self.save()


class BaseTasks(BaseModel):
    tasks: List[BaseTask] = Field(default_factory=list)
    count: int = Field(default=0)
