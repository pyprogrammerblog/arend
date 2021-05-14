import datetime
from arend.settings import base
from arend.queue.task import QueueTask


class ProgressUpdater:

    def __init__(
        self,
        queue_task: QueueTask,
        suppress_exception: bool = True,
        verbose: bool = 1
    ):
        self.queue_task = queue_task
        self.verbose = verbose
        self.suppress_exception = suppress_exception

    def notify(self, message: str):
        self.queue_task.detail += f"- {message}\n"

    def __call__(self, **kwargs):
        self.__dict__.update(kwargs)
        return self

    def __enter__(self):
        self.queue_task.start_time = datetime.datetime.utcnow()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.queue_task.end_time = datetime.datetime.utcnow()
        if exc_type:
            self.queue_task.status = base.FAIL
            self.queue_task.detail = exc_val
            self.queue_task.save()
            return self.suppress_exception
        elif not self._finished:
            self.queue_task.status = base.FINISHED
            self.queue_task.result = exc_val
            self.queue_task.save()
            return True
