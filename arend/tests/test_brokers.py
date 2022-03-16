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


def test_broker_beanstalkd(task, purge_queue):

    with BeanstalkdBroker(queue_name="test") as broker:
        broker.add_to_queue(task_uuid=task.uuid)

        assert broker.stats_tube()
        assert broker.stats_job(task_uuid=task.uuid)

        task_uuid = broker.reserve()
        assert task_uuid == task.uuid

        broker.delete(task_uuid)


def test_broker_redis():
    pass


def test_broker_sqs():
    pass
