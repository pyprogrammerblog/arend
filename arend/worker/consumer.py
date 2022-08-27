from arend.broker import BeanstalkdBroker
from arend.task import Task

import logging
import time


logger = logging.getLogger(__name__)


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

            message = broker.reserve(timeout=timeout)
            if message is None and testing:
                break

            if message:
                queue_task = Task.get(uuid=message.body)
                if queue_task:
                    queue_task.run()  # run sync inside worker

                broker.delete(message)

        time.sleep(sleep_time)
