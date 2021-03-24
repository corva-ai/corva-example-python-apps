import pydantic


class Settings(pydantic.BaseSettings):
    provider: str = 'big-data-energy'
    collection: str = 'formation-evaluation'
    version: int = 1
    app_name: str = 'formation-evaluation-importer-task'
    chunk_size: int = 667


SETTINGS = Settings()
