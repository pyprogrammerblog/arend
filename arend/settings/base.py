from typing import Literal, Union
from pydantic import BaseModel


class STATUS(BaseModel):
    PENDING: str = Literal["PENDING"]
    STARTED: str = Literal["STARTED"]
    RETRY: str = Literal["RETRY"]
    FINISHED: str = Literal["FINISHED"]
    FAIL: str = Literal["FAIL"]
    REVOKED: str = Literal["REVOKED"]

    STATUSES: Union[PENDING, STARTED, RETRY, FINISHED, FAIL, REVOKED] = PENDING


class Settings(BaseModel):

    mongodb_string: str = "pymongo://arend@arend:mongo/"
    mongodb_arend: str = "arend"
    mongodb_arend_task_results: str = "task_results"

    STATUSES: STATUS

