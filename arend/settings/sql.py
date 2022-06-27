from pydantic import BaseModel
from typing import Literal


class SQLSettings(BaseModel):

    # mongo backend settings
    name: Literal["sql"] = "sql"
    connection: str = "mongodb://mongo:mongo@mongo:27017"
    max_pool_size: int = 10
    min_pool_size: int = 0
    db: str = "tasks"
    collection: str = "tasks"
