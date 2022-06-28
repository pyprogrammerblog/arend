from pydantic import BaseSettings


class BeanstalkdSettings(BaseSettings):
    # broker beanstalkd
    beanstalkd_host: str = "beanstalkd"
    beanstalkd_port: int = 11300
    reserve_timeout: int = 20
