from arend.brokers import get_broker
from arend.settings import settings
from arend.tube.task import Task

import logging
import time


logger = logging.getLogger(__name__)


def consumer(queue_name: str, testing: bool = False):
    """
    Single consumer.
    """

    run = True

    while run:

        with get_broker(settings.broker)(queue_name=queue_name) as broker:

            message = broker.reserve()
            if message is None and testing:  # for testing purposes
                run = False
                continue

            if message:
                queue_task = Task.get(uuid=message.body)
                if queue_task:
                    queue_task.run()  # run sync inside worker

                broker.delete(message)

        time.sleep(settings.sleep_time_consumer)
