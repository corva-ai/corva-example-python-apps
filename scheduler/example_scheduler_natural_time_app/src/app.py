import statistics

from corva import Api, Cache, Logger, ScheduledNaturalTimeEvent

from src.configuration import SETTINGS


def example_drilling_scheduler_app(event: ScheduledNaturalTimeEvent, api: Api, cache: Cache):

    # The scheduled app can declare the following attributes from the ScheduledNaturalTimeEvent: 
    # company_id: The company identifier; 
    # asset_id: The asset identifier; 
    # schedule_start: The start time of interval; 
    # interval: The time between two schedule triggers.
    asset_id = event.asset_id
    company_id = event.company_id
    schedule_start = event.schedule_start
    interval = event.interval
    schedule_end = schedule_start + interval

    # Check cache for duplicate data BEFORE making API calls. This prevents unnecessary API requests when data has already been processed.
    # The Cache functionality is built on Redis. See the Cache documentation for more information.
    # Getting last exported timestamp from cache
    last_exported_timestamp = int(float(cache.get(key='last_exported_timestamp') or 0))

    # Skip processing if this time interval was already handled
    if schedule_end <= last_exported_timestamp:
        Logger.debug(f"Already processed data until {last_exported_timestamp=}")
        return None

    # Utilize attributes from the event to fetch the dataset using api.get_dataset.
    # Fetch time-based dataset for the asset based on schedule_start and schedule_end.
    # schedule_start and schedule_end are inclusive so the query is structured accordingly to avoid processing duplicate data.
    # Query only the 'weight_on_bit' field (nested under data) since that is the only field we need. 
    # Use api.get_dataset convenience method (see API docs).
    records = api.get_dataset(
        provider="corva",
        dataset= SETTINGS.wits_collection,
        query={
            'asset_id': asset_id,
            'timestamp': {
                '$gte': schedule_start,
                '$lte': schedule_end
            }
        },
        sort={'timestamp': 1},
        limit=500,
        fields="data.weight_on_bit"
    )

    record_count = len(records)

    # Early exit if no records are found. Prevents errors when accessing records[-1] later and avoids unnecessary processing.
    if not records:
        Logger.debug(f"No records found for {schedule_start=} to {schedule_end=}")
        return None

    # Computing mean weight on bit from the list of realtime wits records
    mean_weight_on_bit = statistics.mean(record.get("data", {}).get("weight_on_bit", 0) for record in records)

    # Building the required output
    output = {
        "timestamp": records[-1].get('timestamp', schedule_end),
        "asset_id": asset_id,
        "company_id": company_id,
        "provider": SETTINGS.provider,
        "collection": SETTINGS.output_collection,
        "data": {
            "mean_weight_on_bit": mean_weight_on_bit,
            "start_time": schedule_start,
            "end_time": schedule_end
        },
        "version": SETTINGS.version
    }

    Logger.debug(f"{asset_id=} {company_id=}")
    Logger.debug(f"{schedule_start=} {schedule_end=} {record_count=}")
    Logger.debug(f"{output=}")

    # Sending a POST request to Corva Data API with the output data that we created above. Please note data is always a list.
    # if request fails, lambda will be re-invoked. so no exception handling
    api.post(
        f"api/v1/data/{SETTINGS.provider}/{SETTINGS.output_collection}/", data=[output],
    ).raise_for_status()

    # Storing the output timestamp to cache
    cache.set(key='last_exported_timestamp', value=str(output.get("timestamp")))

    return output