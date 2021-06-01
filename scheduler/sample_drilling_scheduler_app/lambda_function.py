from corva import Api, Cache, Logger, ScheduledEvent, scheduled

from src.app import sample_drilling_scheduler_app


@scheduled
def lambda_handler(event: ScheduledEvent, api: Api, cache: Cache):

    Logger.info('App Starts')
    return sample_drilling_scheduler_app(event, api, cache)
