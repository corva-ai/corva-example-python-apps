import pydantic


class Settings(pydantic.BaseSettings):
    PROVIDER: str
    API_LIMIT: int = 1000
    IN_DATASET: str = 'drilling.wits.depth'
    OUT_DATASET: str = 'drilling.wits.depth.median-{interval}ft'
    VERSION: int = 1


SETTINGS = Settings()
