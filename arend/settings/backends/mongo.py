from pydantic import BaseSettings
from typing import Literal


class MongoSettings(BaseSettings):

    # mongo backend settings
    backend_type: Literal["mongo"] = "mongo"
    connection: str = "mongodb://mongo:mongo@mongo:27017"
    max_pool_size: int = 10
    min_pool_size: int = 0
    db: str = "tasks"
    collection: str = "tasks"
