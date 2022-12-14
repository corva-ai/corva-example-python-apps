from corva import Api, Cache, Logger, ScheduledDepthEvent, scheduled

from src.app import example_scheduled_depth_app


@scheduled
def lambda_handler(event: ScheduledDepthEvent, api: Api, cache: Cache):
    """Insert your logic here"""
    Logger.info('This is a log file for my depth app!')
    return example_scheduled_depth_app(event, api, cache)
