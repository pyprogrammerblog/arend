from arend.settings import settings
from pystalkd.Beanstalkd import Connection

import logging

__all__ = ["BeanstalkdBroker"]


logger = logging.getLogger(__name__)


class BeanstalkdBroker:
    def __init__(self, queue: str):
        self.queue: str = queue
        self.connection: Connection = Connection(
            host=settings.beanstalkd_host, port=settings.beanstalkd_port
        )
        self.connection.watch(name=queue)
        self.connection.use(name=queue)

    def __enter__(self: "BeanstalkdBroker") -> Connection:
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
