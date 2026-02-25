import statistics

from corva import Api, Cache, Logger, ScheduledDepthEvent
from corva.service.cache_sdk import UserRedisSdk

from src.configuration import SETTINGS


def example_scheduled_depth_app(event: ScheduledDepthEvent, api: Api, cache: Cache):

    # Extract attributes from the ScheduledDepthEvent and start using Api, Cache, and Logger functionalities.
    # The scheduled app can declare the following attributes from the ScheduledDepthEvent: asset_id: The asset identifier; company_id: The company identifier; top_depth: The start depth in ft.; bottom_depth: The end depth in ft.; interval: distance between two schedule triggers; log_identifier: app stream log identifier. Used to scope data by stream. The asset may be connected to multiple depth based logs.
    asset_id = event.asset_id
    company_id = event.company_id
    log_identifier = event.log_identifier
    top_depth = event.top_depth
    bottom_depth = event.bottom_depth
    # interval = event.interval - Not used in this example

    # Check cache for duplicate data BEFORE making API calls. This prevents unnecessary API requests when data has already been processed.
    # The Cache functionality is built on Redis. See the Cache documentation for more information.
    # Getting last exported measured depth from cache
    last_exported_measured_depth = int(float(cache.get(key='measured_depth') or 0))

    # Skip processing if this depth interval was already handled
    if bottom_depth <= last_exported_measured_depth:
        Logger.debug(f"Already processed data until {last_exported_measured_depth=}")
        return None


    # Utilize attributes from the event to fetch the dataset using api.get_dataset.
    # Fetch realtime drilling depth data for the asset based on top_depth and bottom_depth.
    # top_depth and bottom_depth are inclusive so the query is structured accordingly to avoid processing duplicate data.
    # Query the 'rop' and 'measured_depth' fields. Use api.get_dataset convenience method (see API docs).
    records = api.get_dataset(
        provider="corva",
        dataset= SETTINGS.wits_collection,
        query={
            'asset_id': asset_id,
            'log_identifier': log_identifier,
            'measured_depth': {
                '$gte': top_depth,
                '$lte': bottom_depth,
            },
        },
        sort={'measured_depth': 1},
        limit=500,
        fields="data.rop,measured_depth",
    )

    # Early exit if no records are found. Prevents errors when accessing records[-1] later and avoids unnecessary processing.
    if not records:
        Logger.debug(f"No records found for {top_depth=} to {bottom_depth=}")
        return None

    record_count = len(records)

    # Use Logger for debugging/info. To change default log level set "LOG_LEVEL" in manifest.json under "settings.environment".
    Logger.debug(f"{asset_id=} {company_id=}")
    Logger.debug(f"{top_depth=} {bottom_depth=} {record_count=}")


    # Implement calculations: compute mean rop from records
    # Compute mean 'rop' value from the realtime drilling.wits.depth records.
    mean_rop = statistics.mean(record.get("data", {}).get("rop", 0) for record in records)

    # Prepare POST request payload to store mean rop and interval depths.
    output = {
        "measured_depth": records[-1].get("measured_depth"),
        "asset_id": asset_id,
        "company_id": company_id,
        "provider": SETTINGS.provider,
        "collection": SETTINGS.output_collection,
        "log_identifier": log_identifier,
        "data": {
            "mean_rop": mean_rop,
            "top_depth": top_depth,
            "bottom_depth": bottom_depth
        },
        "version": SETTINGS.version
    }

    # Utilize the Logger functionality.
    Logger.debug(f"{asset_id=} {company_id=}")
    Logger.debug(f"{top_depth=} {bottom_depth=} {record_count=}")
    Logger.debug(f"{output=}")

    # Save the calculated data and update cache
    # Utilize the Api functionality. The data=outputs needs to be an array because Corva's data is saved as an array of objects (records). See the Api documentation for more information.
    api.post(
        f"api/v1/data/{SETTINGS.provider}/{SETTINGS.output_collection}/", data=[output],
    ).raise_for_status()

    # Update the cache with the last measured_depth. This value is checked at the start
    # of the function (step 4) to prevent duplicate processing on subsequent invocations.
    # Convert to string as Redis stores values as strings.
    cache.set(key='measured_depth', value=str(output.get("measured_depth")))

    return output