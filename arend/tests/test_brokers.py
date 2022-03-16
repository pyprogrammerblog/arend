from arend.brokers import get_queue_broker
from arend.brokers.beanstalkd import BeanstalkdBroker
from arend.brokers.redis import RedisBroker
from arend.brokers.sqs import SQSBroker


def test_select_brokers():
    broker = get_queue_broker(broker="redis")
    assert broker == RedisBroker

    broker = get_queue_broker(broker="sqs")
    assert broker == SQSBroker

    broker = get_queue_broker(broker="beanstalkd")
    assert broker == BeanstalkdBroker


def test_broker_mongo():
    pass


def test_broker_redis():
    pass


def test_broker_sql():
    pass
