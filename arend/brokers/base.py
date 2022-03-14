import logging
import pystalkd
import uuid


logger = logging.getLogger(__name__)


class BaseBroker:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError

    def add_to_queue(self, task_uuid: uuid.UUID):
        raise NotImplementedError

    def reserve(self, timeout: int = None) -> pystalkd.Job:
        raise NotImplementedError

    def delete(self, job):
        raise NotImplementedError
