from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    provider: str = "big-data-energy"
    output_collection: str = "example-scheduled-natural-time-app"
    wits_collection: str = "example-scheduled-natural-time-app"
    version: int = 1


SETTINGS = Settings()