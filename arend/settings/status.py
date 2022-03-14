from typing import Union
import logging

logger = logging.getLogger(__name__)


SCHEDULED: str = "SCHEDULED"
PENDING: str = "PENDING"
STARTED: str = "STARTED"
RETRY: str = "RETRY"
FINISHED: str = "FINISHED"
FAIL: str = "FAIL"
REVOKED: str = "REVOKED"

STATUSES: Union[
    PENDING, STARTED, RETRY, FINISHED, FAIL, REVOKED, SCHEDULED
] = PENDING
