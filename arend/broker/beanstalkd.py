import logging

from arend.settings import settings
from pystalkd.Beanstalkd import Connection

logger = logging.getLogger(__name__)


__all__ = ["Beanstalkd"]


class Beanstalkd:
    """
    Beanstalkd Broker. Weird that the class does not implement a context manager...
    """

    def __init__(self, queue_name: str):
        self.queue_name = queue_name
        self.connection = Connection(
            host=settings.beanstalkd_host, port=settings.beanstalkd_port
        )
        self.connection.watch(name=self.queue_name)
        self.connection.use(name=self.queue_name)

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def stats_tube(self):
        return self.connection.stats_tube(name=self.queue_name)
