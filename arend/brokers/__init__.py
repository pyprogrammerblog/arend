from .beanstalkd import BeanstalkdBroker
from .redis import RedisBroker
from .sqs import SQSBroker
from functools import lru_cache


@lru_cache
def get_broker(broker):
    brokers = {
        "beanstalkd": BeanstalkdBroker,
        "redis": RedisBroker,
        "sqs": SQSBroker,
    }
    return brokers.get(broker)
