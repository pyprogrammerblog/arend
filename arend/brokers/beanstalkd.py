from arend.brokers.base import BaseBroker
from arend.settings import settings
from pystalkd.Beanstalkd import Connection

import logging
import pystalkd
import uuid


logger = logging.getLogger(__name__)


class BeanstalkdBroker(BaseBroker):
    def __init__(self, queue_name: str):
        self.queue_name = queue_name
        self.connection = Connection(
            host=settings.beanstalkd_host, port=settings.beanstalkd_port
        )
        self.connection.watch(name=queue_name)
        self.connection.use(name=queue_name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def add_to_queue(self, task_uuid: uuid.UUID):
        self.connection.put(body=str(task_uuid))

    def reserve(self) -> pystalkd.Job:
        job = self.connection.reserve(timeout=settings.reserve_timeout)
        return job

    def delete(self, job: pystalkd.Job):
        job.delete()
