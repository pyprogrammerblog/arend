from pydantic import BaseSettings, Field


class BeanstalkdSettings(BaseSettings):
    # broker beanstalkd
    host: str = "beanstalkd"
    port: int = 11300
    reserve_timeout: int = Field(default=20, gte=0, lte=30)
