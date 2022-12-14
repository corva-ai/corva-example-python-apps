import pydantic


class Settings(pydantic.BaseSettings):
    provider: str = "big-data-energy"
    output_collection: str = "example-scheduled-depth-app"
    wits_collection: str = "drilling.wits.depth"
    version: int = 1


SETTINGS = Settings()


