from arend.brokers import BeanstalkdConnection
from arend.settings import Settings, ArendSettings
from uuid import UUID
import logging
import time

__all__ = ["consumer"]


logger = logging.getLogger(__name__)


def consumer(
    queue: str,
    timeout: int = 20,
    sleep_time: float = 0.1,
    long_polling: bool = True,
    settings: ArendSettings = None,
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
        >>>
        >>> consumer(queue="my_queue")
    """

    settings = settings or Settings().arend
    Task = settings.get_backend()

    while True:

        with BeanstalkdConnection(
            queue=queue, settings=settings.beanstalkd
        ) as conn:

            message = conn.reserve(timeout=timeout)
            if message is None and not long_polling:
                break  # if not long_polling, consume all messages and break

            if message:
                if task := Task.get(uuid=UUID(message.body)):
                    task.run()  # run task here

                message.delete()

        time.sleep(sleep_time)
