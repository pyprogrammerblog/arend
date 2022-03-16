from arend.brokers.base import BaseBroker
from arend.settings import settings
from pystalkd.Beanstalkd import Connection

import logging


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

    def add_to_queue(self, task_uuid: str):
        self.connection.put(body=task_uuid)

    def reserve(self) -> str:
        job = self.connection.reserve(timeout=settings.reserve_timeout)
        if job:
            return job.body

    def delete(self, task_uuid: str):
        job = self.connection.parse_job(body=task_uuid)
        job.delete()

    def stats_tube(self):
        return self.connection.stats_tube(name=self.queue_name)

    def stats_job(self, task_uuid: str):
        job = self.connection.parse_job(body=task_uuid)
        return self.connection.stats_job(job_id=job.id)
