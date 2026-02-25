import statistics

from corva import Api, Cache, Logger, ScheduledDataTimeEvent

from src.configuration import SETTINGS


def example_scheduled_data_time_app(event: ScheduledDataTimeEvent, api: Api, cache: Cache):

    # Extract attributes from the ScheduledDataTimeEvent and start using Api, Cache, and Logger functionalities.
    # The scheduled app can declare the following attributes from the ScheduledDataTimeEvent: company_id: The company identifier; asset_id: The asset identifier; start_time: The start time of interval; end_time: The end time of interval
    asset_id = event.asset_id
    company_id = event.company_id
    start_time = event.start_time
    end_time = event.end_time

    # Check cache for duplicate data BEFORE making API calls. This prevents unnecessary API requests when data has already been processed.
    # The Cache functionality is built on Redis. See the Cache documentation for more information.
    last_exported_timestamp = int(float(cache.get(key='last_exported_timestamp') or 0))

    # Skip processing if this time interval was already handled
    if end_time <= last_exported_timestamp:
        Logger.debug(f"Already processed data until {last_exported_timestamp=}")
        return None

    # Utilize attributes from the event to fetch the dataset using api.get_dataset.
    # Fetch realtime drilling data for the asset based on start_time and end_time.
    # start_time and end_time are inclusive so the query is structured accordingly to avoid processing duplicate data.
    # Query only the 'rop' field (nested under data) since that's the only field needed. Use api.get_dataset convenience method (see API docs).
    records = api.get_dataset(
        provider="corva",
        dataset= "wits",
        query={
            'asset_id': asset_id,
            'timestamp': {
                '$gte': start_time,
                '$lte': end_time,
            }
        },
        sort={'timestamp': 1},
        limit=500,
        fields="data.rop"
    )

    # Early exit if no records are found. Prevents errors when accessing records[-1] later and avoids unnecessary processing.
    if not records:
        Logger.debug(f"No records found for {start_time=} to {end_time=}")
        return None

    record_count = len(records)

    # Use Logger for debugging/info. To change default log level set "LOG_LEVEL" in manifest.json under "settings.environment".
    Logger.debug(f"{asset_id=} {company_id=}")
    Logger.debug(f"{start_time=} {end_time=} {record_count=}")


    # Implement calculations: compute mean rop from records
    # Compute mean 'rop' value from the realtime wits records.        
    rop = statistics.mean(record.get("data", {}).get("rop", 0) for record in records)

    # Prepare POST request payload to store mean rop and interval times.
    output = {
        "timestamp": records[-1].get("timestamp", end_time), 
        "asset_id": asset_id,
        "company_id": company_id,
        "provider": SETTINGS.provider, 
        "collection": SETTINGS.output_collection,
        "data": {
            "rop": rop,
            "start_time": start_time,
            "end_time": end_time
        },
        "version": 1
    }

    # Utilize the Logger functionality.
    Logger.debug(f"{asset_id=} {company_id=}")
    Logger.debug(f"{start_time=} {end_time=} {record_count=}")
    Logger.debug(f"{output=}")

    # Save the calculated data and update cache

    # Utilize the Api functionality. The data=outputs needs to be an array because Corva's data is saved as an array of objects (records). See the Api documentation for more information.
    api.post(
        f"api/v1/data/{SETTINGS.provider}/{SETTINGS.output_collection}/", data=[output], 
    ).raise_for_status()

    # Update the cache with the last processed timestamp. This value is checked at the start
    # of the function (step 4) to prevent duplicate processing on subsequent invocations.
    # Convert to string as Redis stores values as strings.
    cache.set(key='last_exported_timestamp', value=str(output.get("timestamp")))

    return output
