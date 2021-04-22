import statistics

from corva import Api, Cache, Logger, ScheduledEvent

from src.configuration import SETTINGS


def sample_drilling_scheduler_app(event: ScheduledEvent, api: Api, cache: Cache):

    # You have access to asset_id, start_time and end_time of the event.
    asset_id = event.asset_id
    start_time = event.start_time
    end_time = event.end_time

    # You have to fetch the realtime drilling data for the asset based on start and end time of the event.
    # start_time and end_time are inclusive so the query is structured accordingly to avoid processing duplicate data
    # We are only querying for weight_on_bit field since that is the only field we need. It is nested under data.
    records = api.get_dataset(
        provider="corva",
        dataset=SETTINGS.wits_collection,
        query={
            'asset_id': asset_id,
            'timestamp': {
                '$gte': start_time,
                '$lte': end_time,
            }
        },
        sort={'timestamp': 1},
        limit=500,
        fields="data.weight_on_bit"
    )

    record_count = len(records)

    # Computing mean weight on bit from the list of realtime wits records
    mean_weight_on_bit = statistics.mean(record.get("data", {}).get("weight_on_bit", 0) for record in records)
    company_id = records[0].get("company_id")

    # Building the required output
    output = {
        "timestamp": end_time,
        "asset_id": asset_id,
        "company_id": company_id,
        "provider": SETTINGS.provider,
        "collection": SETTINGS.output_collection,
        "data": {
            "mean_weight_on_bit": mean_weight_on_bit,
            "start_time": start_time,
            "end_time": end_time
        },
        "version": SETTINGS.version
    }

    Logger.debug(f"asset_id {asset_id} company_id {company_id}")
    Logger.debug(f"start_timestamp {start_time} end_timestamp {end_time} record_count {record_count}")

    # if request fails, lambda will be reinvoked. so no exception handling
    api.post(
        f"api/v1/data/{SETTINGS.provider}/{SETTINGS.output_collection}/", data=[output],
    ).raise_for_status()
