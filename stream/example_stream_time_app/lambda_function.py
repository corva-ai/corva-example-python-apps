from corva import Api, Cache, Logger, StreamTimeEvent, stream

from src.app import example_stream_app

@stream
def lambda_handler(event: StreamTimeEvent, api: Api, cache: Cache):
    """Insert your logic here"""
    Logger.info('Hello, World!')
    return example_stream_app(event, api, cache)
