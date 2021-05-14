import asyncio
from typing import Union
from typing import Callable
from datetime import timedelta
from pydantic import BaseModel
from pystalkd.Beanstalkd import DEFAULT_PRIORITY
from arend.queue.beanstalkd import BeanstalkdConnector
from arend.backend.mongo import MongoTasksConnector
from arend.queue.task import QueueTask


class AsyncTask(BaseModel):
    task_name: str
    task_location: str
    processor: Callable
    args: tuple
    kwargs: dict
    queue_name: str
    queue_priority: int
    delay: int

    def __call__(self, *args, **kwargs):
        self.run(*args, **kwargs)

    def __repr__(self):
        return f"<{self.__class__.__name__} at {self.task_location}>"

    def run(self, *args, **kwargs):
        """
        Run the task immediately.
        """
        if asyncio.iscoroutinefunction(self.processor):
            return asyncio.run(self.processor(*args, **kwargs))
        else:
            return self.processor(*args, **kwargs)

    def apply_async(
        self,
        queue_name: str = None,
        queue_priority: str = DEFAULT_PRIORITY,
        delay: Union[timedelta, int] = 0,
        args: tuple = None,
        kwargs: dict = None,
    ):
        """
        Run task asynchronously.
        """
        queue_name = self.queue_name or queue_name
        assert self.queue_name, "Queue name is not defined."
        queue_priority = self.queue_priority or queue_priority
        delay = self.delay or delay

        with MongoTasksConnector() as conn:
            queue_task = QueueTask(
                task_name=self.task_name,
                task_location=self.task_location,
                queue_name=queue_name,
                queue_priority=queue_priority,
                delay=delay,
                args=args,
                kwargs=kwargs,
            )
            inserted_id = conn.task_collection.insert_one(queue_task.dict())

        with BeanstalkdConnector(queue_name=queue_name) as c:
            c.put(body=str(inserted_id), priority=queue_priority, delay=delay)
