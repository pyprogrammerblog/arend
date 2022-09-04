from arend.broker import BeanstalkdBroker
from arend.backend.task import Task
from uuid import UUID
import logging
import time


logger = logging.getLogger(__name__)

__all__ = ["consumer"]


def consumer(
    queue: str,
    timeout: int = 20,
    long_polling: bool = False,
    sleep_time: int = 1,
):
    """

    Args:
        queue:
        timeout:
        long_polling:
        sleep_time:

    Returns:

    """

    while True:

        with BeanstalkdBroker(queue=queue) as broker:

            message = broker.connection.reserve(timeout=timeout)
            if message is None and not long_polling:
                # if not long_polling, consume all messages and break loop
                break

            if message:
                queue_task = Task.get(uuid=UUID(message.body))
                if queue_task:
                    queue_task.run()  # run task here

                message.delete()

        time.sleep(sleep_time)
