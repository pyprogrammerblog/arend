import logging
import time

from arend.backends import Task
from arend.beanstalkd import BeanstalkdBroker
from arend.settings import settings

__all__ = ["consumer"]


logger = logging.getLogger(__name__)


def consumer(queue_name: str, logger: logging.Logger):
    """
    Single consumer.
    """

    while True:

        with BeanstalkdBroker(queue_name=queue_name) as broker:

            job = broker.reserve()
            task = Task.get(uuid=job.body)
            if task:
                task.run()  # run sync inside worker

            job.delete()

        time.sleep(settings.sleep_time_consumer)
