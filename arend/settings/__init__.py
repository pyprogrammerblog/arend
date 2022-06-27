from arend.settings.settings import ArendSettings
from functools import lru_cache


@lru_cache
def get_settings() -> ArendSettings:
    return ArendSettings()


settings = get_settings()
