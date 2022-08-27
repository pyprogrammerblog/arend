from arend.broker import BeanstalkdBroker
from arend.backend.task import Task
from uuid import UUID
import logging
import time


logger = logging.getLogger(__name__)

__all__ = ["consumer"]


def consumer(
    queue_name: str,
    timeout: int = 20,
    testing: bool = False,
    sleep_time: int = 1,
):
    """
    Single consumer.
    """

    while True:

        with BeanstalkdBroker(queue_name=queue_name) as broker:

            message = broker.connection.reserve(timeout=timeout)
            if message is None and testing:
                break

            if message:
                queue_task = Task.get(uuid=UUID(message.body))
                if queue_task:
                    queue_task.run()  # run sync inside worker

                message.delete()

        time.sleep(sleep_time)
