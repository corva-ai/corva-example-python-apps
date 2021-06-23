import pydantic


class Settings(pydantic.BaseSettings):
    provider: str
    output_collection: str = "sample-drilling-scheduler-collection"
    wits_collection: str = "wits"
    version: int = 1


SETTINGS = Settings()
