import logging

from arend.settings import settings
from pystalkd import Job
from pystalkd.Beanstalkd import Connection

logger = logging.getLogger(__name__)


class BeanstalkdBroker:
    """
    Beanstalkd Broker
    """

    def __init__(self, queue_name: str):
        self.queue_name = queue_name
        self.connection = Connection(
            host=settings.beanstalkd_host, port=settings.beanstalkd_port
        )
        self.connection.watch(name=self.queue_name)
        self.connection.use(name=self.queue_name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def add_to_queue(self, task_uuid: str):
        self.connection.put(body=task_uuid)

    def reserve(self) -> Job:
        job = self.connection.reserve(timeout=settings.reserve_timeout)
        return job

    def delete(self, job: Job):
        job.delete()

    def stats_tube(self):
        return self.connection.stats_tube(name=self.queue_name)

    def stats_job(self, job: Job):
        return self.connection.stats_job(job_id=job.job_id)
