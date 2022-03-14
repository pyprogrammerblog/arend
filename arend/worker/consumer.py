from arend.brokers import QueueBroker
from arend.settings import settings
from arend.tube.task import Task

import logging
import time


logger = logging.getLogger(__name__)


def consumer(queue_name: str, timeout: int = 20, testing: bool = False):
    """
    Single consumer.

    :param queue_name:
    :param timeout:
    :param testing:
    :return:
    """

    run = True

    while run:

        with QueueBroker(queue_name=queue_name) as broker:

            message = broker.reserve(timeout=timeout)
            if message is None and testing:  # for testing purposes
                run = False
                continue

            if message:
                queue_task = Task.get(uuid=message.body)
                if queue_task:
                    queue_task.run()  # run sync inside worker

                broker.delete(message)

        time.sleep(settings.sleep_time_consumer)
