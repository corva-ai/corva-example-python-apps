from corva import Api, Cache, Logger, StreamTimeEvent, stream

from src.app import sample_drilling_stream_app


@stream
def lambda_handler(event: StreamTimeEvent, api: Api, cache: Cache):

    Logger.info('App Starts')
    return sample_drilling_stream_app(event, api, cache)
