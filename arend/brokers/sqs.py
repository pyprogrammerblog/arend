from arend.brokers.base import BaseBroker

import logging
import pystalkd
import uuid


logger = logging.getLogger(__name__)


class SQSBroker(BaseBroker):
    def __init__(self, queue_name: str):
        self.queue_name = queue_name

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def add_to_queue(self, task_uuid: uuid.UUID):
        pass

    def reserve(self, timeout: int = None):
        pass

    def delete(self, job: pystalkd.Job):
        pass
