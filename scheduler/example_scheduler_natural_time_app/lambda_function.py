from corva import Api, Cache, Logger, ScheduledNaturalTimeEvent, scheduled

from src.app import example_drilling_scheduler_app

@scheduled
def lambda_handler(event: ScheduledNaturalTimeEvent, api: Api, cache: Cache):
    """Insert your logic here"""
    Logger.info('This is my natural time scheduler app log!')
    return example_drilling_scheduler_app(event, api, cache)

