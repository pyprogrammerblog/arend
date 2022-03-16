import logging


logger = logging.getLogger(__name__)


class TasksBackend:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def find_one(self, uuid: str):
        pass

    def update_one(self, uuid: str, update: dict):
        pass

    def delete_one(self, uuid: str):
        pass
