import pydantic


class Settings(pydantic.BaseSettings):
    provider: str = 'corva'
    collection: str = 'formation-evaluation'
    version: int = 1
    app_name: str = 'formation-evaluation-importer-task'
    chunk_size: int = 667


SETTINGS = Settings()
