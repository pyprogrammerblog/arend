from arend.backends import get_backend
from arend.brokers import get_broker
from arend.settings import settings
from arend.settings.status import FAIL
from arend.settings.status import FINISHED
from arend.settings.status import PENDING
from arend.settings.status import RETRY
from arend.settings.status import REVOKED
from arend.settings.status import SCHEDULED
from arend.settings.status import STARTED
from datetime import datetime
from pydantic import BaseModel
from pydantic import Field
from sqlalchemy.ext.declarative import declarative_base
from typing import Any
from typing import Optional
from uuid import uuid4

import logging
import traceback


Base = declarative_base()


logger = logging.getLogger(__name__)


DEFAULT_TTR = 30 * 60  # 30 min
Backend = get_backend(settings.backend)
Broker = get_broker(settings.broker)


class Task(BaseModel):
    uuid: str = Field(default_factory=lambda: str(uuid4()), description="ID")
    name: str = Field(description="Full path task name.")
    location: str = Field(description="Full path task location.")
    status: str = Field(default=SCHEDULED, description="Current status.")
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

    class Config:
        orm_mode = True

    @classmethod
    def get(cls, uuid: str):
        with Backend() as backend:
            queue_task = backend.find_one(uuid=uuid)

        if queue_task:
            return Task(**queue_task)

    def save(self):
        with Backend() as backend:
            self.updated = datetime.utcnow()
            backend.update_one(task_uuid=self.uuid, to_update=self.dict())
        return self

    def send_to_queue(self):
        with Broker(queue_name=self.queue_name) as broker:
            broker.add_to_queue(
                body=self.uuid,
                priority=self.priority,
                delay=self.delay + self.count_retries * settings.delay_factor,
                ttr=DEFAULT_TTR,
            )
        self.status = PENDING
        self.save()

    def notify(self, message: str):
        self.detail += f"- {message}\n"

    def __enter__(self):
        self.start_time = datetime.utcnow()
        self.status = STARTED
        self.save()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            last_trace = "".join(traceback.format_tb(exc_tb)).strip()
            self.detail = f"Failure: {last_trace}\n"
            if self.count_retries < settings.task_max_retries:
                self.count_retries += 1
                self.status = RETRY
                self.save()
                # put in the tube again
                self.send_to_queue()
            else:
                self.end_time = datetime.utcnow()
                self.status = FAIL
            self.save()
            return True

    def __call__(self):
        return self.run()

    def run(self):

        with self:
            if self.status == [REVOKED, FAIL, FINISHED]:
                return

            from arend.tasks.registered_tasks import registered_tasks

            registered = registered_tasks(locations=["python"])
            task = registered[self.location]

            result = task.run(*self.args, **self.kwargs)

            # update finished task
            self.status = FINISHED
            self.end_time = datetime.utcnow()
            self.result = result
            self.save()
