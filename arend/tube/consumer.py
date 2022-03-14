from arend.brokers.beanstalkd import BeanstalkdConnector
from arend.tube.task import QueueTask

import logging
import time


logger = logging.getLogger(__name__)


def consumer(queue_name: str, timeout: int = 20, testing: bool = False):
    """

    :param queue_name: str.
    :param timeout: int.
    :param testing: bool.
    """

    run = True

    while run:

        with BeanstalkdConnector(queue_name=queue_name) as beanstalk:

            message = beanstalk.reserve(timeout=timeout)
            if message is None and testing:  # for testing purposes
                run = False
                continue

            if message:
                queue_task = QueueTask.get(uuid=message.body)
                if queue_task:
                    queue_task.run()  # run sync inside worker

                message.delete()

        time.sleep(1)
