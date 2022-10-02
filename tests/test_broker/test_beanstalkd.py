import pytest
from uuid import uuid4
from arend.brokers.beanstalkd import BeanstalkdConnection
from pystalkd.Beanstalkd import CommandFailed


def test_beanstalkd_broker(beanstalkd_setting):

    body = str(uuid4())

    with BeanstalkdConnection(
        queue="test", settings=beanstalkd_setting
    ) as conn:

        conn.put(body=body)

        stats = conn.stats()
        assert stats["current-jobs-urgent"] == 0
        assert stats["current-jobs-ready"] == 1
        assert stats["current-jobs-reserved"] == 0
        assert stats["current-jobs-delayed"] == 0
        assert stats["current-jobs-buried"] == 0

        tube_stats = conn.stats_tube(name="test")
        assert tube_stats["current-jobs-ready"] == 1
        assert tube_stats["total-jobs"] == 1
        assert tube_stats["current-using"] == 1
        assert tube_stats["current-watching"] == 1

        job = conn.reserve()

        assert job.body == body
        assert conn.stats_job(job_id=job.job_id)

        job.delete()

        with pytest.raises(CommandFailed):
            job.delete()


def test_beanstalkd_broker_put_messages(beanstalkd_setting):

    with BeanstalkdConnection(
        queue="test", settings=beanstalkd_setting
    ) as conn:
        body = str(uuid4())
        conn.put(body=body)
        job = conn.reserve()
        assert job.body == body
