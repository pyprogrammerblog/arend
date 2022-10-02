import logging


logger = logging.getLogger(__name__)


__all__ = [
    "TASK_MAX_RETRIES",
    "TASK_DELAY",
    "TASK_PRIORITY",
    "TASK_DELAY_FACTOR",
    "TASK_RETRY_BACKOFF_FACTOR",
]

# settings
TASK_MAX_RETRIES = 3
TASK_RETRY_BACKOFF_FACTOR = 1
TASK_PRIORITY = 1
TASK_DELAY_FACTOR: int = 1
TASK_DELAY: int = 0
