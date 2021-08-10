import pydantic


class Settings(pydantic.BaseSettings):
    PROVIDER: str
    RECORD_LIMIT: int = 1000
    INPUT_DATASET: str = 'drilling.wits.depth'
    OUTPUT_DATASET: str = 'drilling.wits.depth.median-{interval}ft'
    VERSION: int = 1


SETTINGS = Settings()
