import statistics

from corva import Api, Cache, Logger, ScheduledDepthEvent
from corva.service.cache_sdk import UserRedisSdk

from src.configuration import SETTINGS


def example_scheduled_depth_app(event: ScheduledDepthEvent, api: Api, cache: Cache):

    # You have access to asset_id, log_identifier, top_depth and bottom_depth of the event.
    asset_id = event.asset_id
    log_identifier = event.log_identifier
    top_depth = event.top_depth
    bottom_depth = event.bottom_depth

    # You have to fetch the realtime drilling data for the asset based on top_depth and bottom_depth of the event.
    # top_depth and bottom_depth are inclusive so the query is structured accordingly to avoid processing duplicate data
    records = api.get_dataset(
        provider="corva",
        dataset=SETTINGS.wits_collection,
        query={
            'asset_id': asset_id,
            'log_identifier': log_identifier,
            'measured_depth': {
                '$gte': top_depth,
                '$lte': bottom_depth,
            },
        },
        sort={'measured_depth': 1},
        limit=500
    )

    Logger.debug(f"{records}")

    record_count = len(records)
    # Getting custom values from our record. It is up to your which ones you need for your apps. In this case we are getting company_id and dep values.   
    company_id = records[0].get("company_id")
    dep = records[0].get("data", {}).get("dep", 0)
    


    # Getting last exported timestamp from redis
    last_exported_measured_depth = int(cache.get(key='measured_depth') or 0)

    # Making sure we are not processing duplicate data
    if bottom_depth <= last_exported_measured_depth:
        Logger.debug(f"Already processed data until {last_exported_measured_depth=}")
        return None

    # Aggregating everything into one json object.
    output = {
        "measured_depth": bottom_depth,
        "asset_id": asset_id,
        "company_id": company_id,
        "log_identifier": log_identifier,
        "provider": SETTINGS.provider,
        "collection": SETTINGS.output_collection,
        "data": {"dep": dep},
        "version": SETTINGS.version
    }

    Logger.debug(f"{asset_id=} {company_id=} {output}")

    Logger.debug(f"{asset_id=} {company_id=}")
    Logger.debug(f"{top_depth=} {bottom_depth=} {record_count=}")
    Logger.debug(f"{output=}")

    # Sending a POST request to Corva DATA API with the output json we created above. 
    # If request fails, lambda will be re-invoked. so no exception handling. 
    # Please note that the data is always a list.
    api.post(
        f"api/v1/data/{SETTINGS.provider}/{SETTINGS.output_collection}/", data=[output],
    ).raise_for_status()

    # Storing the output timestamp to cache
    cache.set(key='measured_depth', value=output.get("measured_depth"))

    return output