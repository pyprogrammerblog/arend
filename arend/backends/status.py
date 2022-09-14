import logging

logger = logging.getLogger(__name__)


SCHEDULED: str = "SCHEDULED"
PENDING: str = "PENDING"
STARTED: str = "STARTED"
RETRY: str = "RETRY"
FINISHED: str = "FINISHED"
FAIL: str = "FAIL"
REVOKED: str = "REVOKED"
