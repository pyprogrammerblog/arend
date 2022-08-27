from arend.broker import BeanstalkdBroker
from arend.backend.adapters.mongo import DBAdapter
from arend.settings import settings
from arend.settings import status
from datetime import datetime
from pydantic import BaseModel
from pydantic import Field
from typing import Any
from typing import Optional, List
from uuid import uuid4

import logging
import traceback


logger = logging.getLogger(__name__)


DEFAULT_TTR = 30 * 60  # 30 min


class Task(DBAdapter):
    uuid: str = Field(default_factory=lambda: str(uuid4()), description="ID")
    name: str = Field(description="Full path task name.")
    location: str = Field(description="Full path task location.")
    status: str = Field(
        default=status.SCHEDULED, description="Current status."
    )
    result: Optional[Any] = Field(default=None, description="Task result.")
    detail: str = Field(default="", description="Task details.")
    start_time: Optional[datetime] = Field(
        default=None, description="Datetime when task is STARTED."
    )
    end_time: Optional[datetime] = Field(
        default=None,
        description="Datetime when task is FINISHED, FAIL, or REVOKED.",
    )
    args: tuple = Field(default_factory=tuple, description="Task args.")
    kwargs: dict = Field(default_factory=dict, description="Task arguments.")

    queue_name: str = Field(description="Queue name.")
    priority: int = Field(description="Queue priority.")
    delay: int = Field(description="Queue delay.")

    created: datetime = Field(default_factory=datetime.utcnow)
    updated: datetime = None
    exclusive: bool = False
    count_retries: int = 0

    def send_to_queue(self):
        with BeanstalkdBroker(queue_name=self.queue_name) as broker:
            broker.connection.put(
                body=self.uuid,
                priority=self.priority,
                delay=self.delay + self.count_retries * settings.delay_factor,
                ttr=DEFAULT_TTR,
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
                # put in the tube again
                self.send_to_queue()
            else:
                self.end_time = datetime.utcnow()
                self.status = status.FAIL
            self.save()
            return True

    def __call__(self):
        return self.run()

    def run(self):

        with self:
            if self.status == [status.REVOKED, status.FAIL, status.FINISHED]:
                return

            from arend.utils.registered_tasks import registered_tasks

            registered = registered_tasks(locations=["notifier"])
            task = registered[self.location]

            result = task.run(*self.args, **self.kwargs)

            # update finished task
            self.status = status.FINISHED
            self.end_time = datetime.utcnow()
            self.result = result
            self.save()


class Tasks(BaseModel):
    tasks: List[Task] = Field(default_factory=list)
    count: int = Field(default=0)
