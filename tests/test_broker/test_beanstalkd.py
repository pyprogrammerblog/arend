import pytest
from uuid import uuid4
from arend.broker.beanstalkd import BeanstalkdBroker
from pystalkd.Beanstalkd import CommandFailed


def test_beanstalkd_broker(env_vars_mongo, flush_queue):

    body = str(uuid4())

    with BeanstalkdBroker(queue="test") as conn:

        conn.connection.put(body=body)

        stats = conn.connection.stats()
        assert stats["current-jobs-urgent"] == 0
        assert stats["current-jobs-ready"] == 1
        assert stats["current-jobs-reserved"] == 0
        assert stats["current-jobs-delayed"] == 0
        assert stats["current-jobs-buried"] == 0

        tube_stats = conn.connection.stats_tube(name="test")
        assert tube_stats["current-jobs-ready"] == 1
        assert tube_stats["total-jobs"] == 1
        assert tube_stats["current-using"] == 1
        assert tube_stats["current-watching"] == 1

        job = conn.connection.reserve()

        assert job.body == body
        assert conn.connection.stats_job(job_id=job.job_id)

        assert conn.settings.broker.host == "beanstalkd"
        assert conn.settings.broker.port == 11300

        job.delete()

        with pytest.raises(CommandFailed):
            job.delete()
