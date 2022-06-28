from typing import Dict
from pydantic import BaseSettings, Field
from arend.backends import Backends
from arend.settings.broker.beanstalkd import BeanstalkdSettings


class ArendSettings(BaseSettings):

    # general task settings
    task_max_retries: int = 10
    task_priority: int = None
    task_delay: int = None
    task_delay_factor: int = 10

    # queues: key: name of the queue, value: concurrency
    queues: Dict[str, int] = None

    # backends
    backend: Backends = Field(discriminator="backend_type")

    # broker
    broker: BeanstalkdSettings = BeanstalkdSettings()
