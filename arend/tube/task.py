from arend.brokers.beanstalkd import BeanstalkdConnector
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
from pymongo import MongoClient
from typing import Any
from typing import Optional
from uuid import uuid4

import logging
import traceback


logger = logging.getLogger(__name__)


DEFAULT_TTR = 30 * 60  # 30 min


class QueueTask(BaseModel):
    uuid: str = Field(default_factory=lambda: str(uuid4()), description="ID")
    task_name: str = Field(description="Full path task name.")
    task_location: str = Field(description="Full path task location.")
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
    task_priority: int = Field(description="Queue priority.")
    task_delay: int = Field(description="Queue delay.")

    created: datetime = Field(default_factory=datetime.utcnow)
    updated: datetime = None
    exclusive: bool = False
    count_retries: int = 0

    @classmethod
    def get(cls, uuid: str):
        with MongoClient(settings.mongodb_string) as connection:
            db = connection[settings.mongodb_notifier]
            queue_task = db[settings.mongodb_notifier_task_results].find_one(
                {"uuid": uuid}
            )

        if queue_task:
            return QueueTask(**queue_task)

    def save(self):
        with MongoClient(settings.mongodb_string) as connection:
            db = connection[settings.mongodb_notifier]
            self.updated = datetime.utcnow()
            db[settings.mongodb_notifier_task_results].update_one(
                filter={"uuid": self.uuid},
                update={"$set": self.dict()},
                upsert=True,
            )
        return self

    def send_to_queue(self):
        with BeanstalkdConnector(queue_name=self.queue_name) as tube:
            tube.put(
                body=self.uuid,
                priority=self.task_priority,
                delay=self.task_delay
                + self.count_retries * settings.delay_factor,
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

            registered = registered_tasks(locations=["notifier"])
            task = registered[self.task_location]

            result = task.run(*self.args, **self.kwargs)

            # update finished task
            self.status = FINISHED
            self.end_time = datetime.utcnow()
            self.result = result
            self.save()
