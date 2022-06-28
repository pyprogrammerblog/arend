from pydantic import BaseSettings
from typing import Literal


class RabbitMQSettings(BaseModel):

    # mongo backend settings
    backend_type: Literal["rabbitmq"] = "rabbitmq"
    connection: str = "mongodb://mongo:mongo@mongo:27017"
    max_pool_size: int = 10
    min_pool_size: int = 0
    db: str = "tasks"
    collection: str = "tasks"
