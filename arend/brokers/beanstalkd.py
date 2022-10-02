from pystalkd.Beanstalkd import Connection
from pydantic import BaseModel
from pydantic import Field

import logging

__all__ = ["BeanstalkdConnection", "BeanstalkdSettings"]


logger = logging.getLogger(__name__)


class BeanstalkdSettings(BaseModel):
    """
    Defines settings for the Beanstalkd Queue
    """

    host: str = Field(description="Beanstalkd Host")
    port: int = Field(description="Beanstalkd Port")


class BeanstalkdConnection:
    """
    Beanstalkd Connection.

    Defines a context manager to open and close connection
    when interacting with our queue.

    Usage:
        >>> from arend.brokers import BeanstalkdConnection, BeanstalkdSettings
        >>> from arend.backends.mongo import MongoSettings
        >>>
        >>> settings = BeanstalkdSettings(host="beanstalkd", port=11300)
        >>> with BeanstalkdConnection(
        >>>     queue="my_queue",
        >>>     settings=settings
        >>> ) as conn:
        >>>     conn.put(body="my message")
        >>>     ...
        >>>
    """

    def __init__(self, queue: str, settings: BeanstalkdSettings):
        self.settings = settings
        self.connection = Connection(
            host=self.settings.host, port=self.settings.port
        )
        self.connection.watch(queue)
        self.connection.use(queue)

    def __enter__(self: "BeanstalkdConnection") -> Connection:
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
