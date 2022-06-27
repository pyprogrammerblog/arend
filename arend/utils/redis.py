import logging

import redis
from arend.settings.redis import RedisSettings

__all__ = ["get_redis"]

logger = logging.getLogger(__name__)


def get_redis(settings: RedisSettings) -> redis.Redis:
    """

    :param settings:
    :return:
    """

    redis_client = redis.Redis(
        host=settings.redis_host,
        db=settings.redis_db,
        password=settings.redis_password,
        socket_timeout=settings.socket_timeout,
        socket_connect_timeout=settings.socket_connect_timeout,
    )

    return redis_client
