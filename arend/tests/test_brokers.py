from arend.brokers import get_queue_broker
from arend.brokers.beanstalkd import BeanstalkdBroker
from arend.brokers.redis import RedisBroker
from arend.brokers.sqs import SQSBroker
from arend.settings import settings


def test_select_brokers():
    broker = get_queue_broker(broker="redis")
    assert broker == RedisBroker

    broker = get_queue_broker(broker="sqs")
    assert broker == SQSBroker

    broker = get_queue_broker(broker="beanstalkd")
    assert broker == BeanstalkdBroker


def test_broker_beanstalkd(task, purge_queue):

    settings.reserve_timeout = 0

    with BeanstalkdBroker(queue_name="test") as broker:
        broker.add_to_queue(task_uuid=task.uuid)

        job = broker.reserve()
        assert job.body == task.uuid

        stats_tube = broker.stats_tube()
        assert stats_tube["name"] == "test"
        assert stats_tube["current-using"] == 1
        assert stats_tube["current-watching"] == 1
        assert stats_tube["total-jobs"] == 1
        assert stats_tube["current-jobs-reserved"] == 1
        assert stats_tube["current-jobs-ready"] == 0

        stats_job = broker.stats_job(job=job)
        assert stats_job["tube"] == "test"
        assert stats_job["state"] == "reserved"

        broker.delete(job=job)


def test_broker_redis():
    pass


def test_broker_sqs():
    pass
