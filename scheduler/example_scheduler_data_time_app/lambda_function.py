from corva import Api, Cache, Logger, ScheduledDataTimeEvent, scheduled


from src.app import example_scheduled_data_time_app

@scheduled
def lambda_handler(event: ScheduledDataTimeEvent, api: Api, cache: Cache):
    """Insert your logic here"""
    Logger.info("This is a log line for my example schedule app")
    return example_scheduled_data_time_app(event, api, cache)