import pydantic


class Settings(pydantic.BaseSettings):
    provider: str
    output_collection: str = "sample-drilling-stream-collection"
    version: int = 1


SETTINGS = Settings()
