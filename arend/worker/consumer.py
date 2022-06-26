import logging
import time

from arend.broker import get_broker
from arend.settings import settings
from arend.tube.task import Task

logger = logging.getLogger(__name__)


def consumer(queue_name: str):
    """
    Single consumer.
    """

    run = True

    while run:

        with get_broker(settings.broker)(queue_name=queue_name) as broker:

            message = broker.reserve()
            if message is None and settings.consumer_testing:  # for testing
                run = False
                continue

            if message:
                queue_task = Task.get(uuid=message.body)
                if queue_task:
                    queue_task.run()  # run sync inside worker

                broker.delete(message)

        time.sleep(settings.sleep_time_consumer)
