from arend.settings import settings
from pystalkd.Beanstalkd import Connection

import logging

__all__ = ["BeanstalkdBroker"]


logger = logging.getLogger(__name__)


class BeanstalkdBroker:
    def __init__(self, queue_name: str):
        self.queue_name: str = queue_name
        self.connection: Connection = Connection(
            host=settings.beanstalkd_host, port=settings.beanstalkd_port
        )
        self.connection.watch(name=queue_name)
        self.connection.use(name=queue_name)

    def __enter__(self: "BeanstalkdBroker") -> "BeanstalkdBroker":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
