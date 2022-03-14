from arend.settings import settings
from pystalkd.Beanstalkd import Connection

import logging


logger = logging.getLogger(__name__)


class MyConnection(Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class BeanstalkdConnector:
    def __init__(self, queue_name: str):
        self.connection = MyConnection(
            host=settings.beanstalkd_host, port=settings.beanstalkd_port
        )
        self.connection.watch(name=queue_name)
        self.connection.use(name=queue_name)

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
