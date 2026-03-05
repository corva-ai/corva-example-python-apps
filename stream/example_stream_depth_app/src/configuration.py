from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    provider: str = "big-data-energy"
    output_collection: str = "example-stream-depth-app"
    version: int = 1


SETTINGS = Settings()