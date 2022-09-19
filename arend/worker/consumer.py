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
    sleep_time: float = 0.1,
    long_polling: bool = False,
    settings: Union[MongoSettings, RedisSettings, SQLSettings, None] = None,
):
    """
    Consumer. Consume messages from the queue.

    Args:
        queue: str. Queue name.
        timeout: int. Polling timeout.
        sleep_time: float. Sleeping time between polling cycles.
        long_polling: bool. Break the loop if no more messages.
        settings: MongoSettings, RedisSettings, SQLSettings, None.
            Backend settings. If no settings are passed, the consumer
            will try to get them from env variables.

    Usage:
        >>> from arend.worker.consumer import consumer
        >>> from arend.backends.mongo import MongoSettings
        >>>
        >>> settings = MongoSettings(
        >>>     mongo_connection="mongodb://user:pass@mongo:27017",
        >>>     mongo_db="db",
        >>>     mongo_collection="logs",
        >>> )
        >>> consumer(queue="my_queue", long_polling=True, settings=settings)
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
