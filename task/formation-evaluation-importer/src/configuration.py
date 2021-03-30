import pydantic


class Settings(pydantic.BaseSettings):
    provider: str = 'big-data-energy'
    collection: str = 'formation-evaluation'
    version: int = 1
    app_name: str = 'formation-evaluation-importer-task'
    chunk_size: int = 667

    @property
    def data_collection(self) -> str:
        return f'{self.collection}.data'

    @property
    def metadata_collection(self) -> str:
        return f'{self.collection}.metadata'


SETTINGS = Settings()
