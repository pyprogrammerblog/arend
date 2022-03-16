from arend.brokers import get_queue_broker
from arend.brokers.beanstalkd import BeanstalkdBroker
from arend.brokers.redis import RedisBroker
from arend.brokers.sqs import SQSBroker


def test_select_brokers():
    backend = get_queue_broker(broker="redis")
    assert backend == RedisBroker

    backend = get_queue_broker(broker="sql")
    assert backend == SQSBroker

    backend = get_queue_broker(broker="beanstalkd")
    assert backend == BeanstalkdBroker


def test_broker_mongo():
    pass


def test_broker_redis():
    pass


def test_broker_sql():
    pass
