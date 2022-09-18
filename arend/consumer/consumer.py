from arend.broker import BeanstalkdBroker
from arend.backends.mongo import MongoSettings
from arend.backends.sql import SQLSettings
from arend.backends.redis import RedisSettings
from arend.backends import Settings
from typing import Union
from uuid import UUID
import logging
import time

__all__ = ["consumer"]


logger = logging.getLogger(__name__)


def consumer(
    queue: str,
    timeout: int = 20,
    long_polling: bool = False,
    sleep_time: float = 1,
    settings: Union[MongoSettings, RedisSettings, SQLSettings, None] = None,
):
    """
    Consumer
    """

    settings = settings or Settings()
    Task = settings.backend()

    while True:

        with BeanstalkdBroker(queue=queue) as broker:

            message = broker.reserve(timeout=timeout)
            if message is None and not long_polling:
                # if not long_polling, consume all messages and break loop
                break

            if message:
                task = Task.get(uuid=UUID(message.body))
                if task:
                    task.run()  # run task here

                message.delete()

        time.sleep(sleep_time)
