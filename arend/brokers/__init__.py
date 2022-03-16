from .beanstalkd import BeanstalkdBroker
from .redis import RedisBroker
from .sqs import SQSBroker
from arend.settings import settings
from functools import lru_cache


@lru_cache
def get_queue_broker(broker: str):
    brokers = {
        "beanstalk": BeanstalkdBroker,
        "redis": RedisBroker,
        "sqs": SQSBroker,
    }
    return brokers.get(broker)


Broker = get_queue_broker(broker=settings.broker)
