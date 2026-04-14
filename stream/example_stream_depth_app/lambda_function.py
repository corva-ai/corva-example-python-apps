from corva import Api, Cache, Logger, StreamDepthEvent, stream

from src.app import example_stream_depth_app


@stream
def lambda_handler(event: StreamDepthEvent, api: Api, cache: Cache):
    """Insert your logic here"""
    Logger.info('Hello, World!')
    return example_stream_depth_app(event, api, cache)
