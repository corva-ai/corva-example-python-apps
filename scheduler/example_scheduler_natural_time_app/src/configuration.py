import pydantic


class Settings(pydantic.BaseSettings):
    provider: str = "big-data-energy"
    output_collection: str = "example-scheduled-natural-time-app"
    version: int = 1


SETTINGS = Settings()