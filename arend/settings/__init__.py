from functools import lru_cache
from arend.settings.base import Settings


@lru_cache
def get_settings():



    return Settings(**map_secrets)


settings = get_settings()
