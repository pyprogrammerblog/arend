from .beanstalkd import BeanstalkdBroker
from .redis import RedisBroker
from .sqs import SQSBroker
from arend.settings import settings
from functools import lru_cache


@lru_cache
def get_queue_broker():
    brokers = {
        "beanstalk": BeanstalkdBroker,
        "redis": RedisBroker,
        "sqs": SQSBroker,
    }
    return brokers.get(settings.broker)


QueueBroker = get_queue_broker()
