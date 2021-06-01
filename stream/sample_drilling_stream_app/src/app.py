from corva import Api, Cache, Logger, StreamTimeEvent

from src.configuration import SETTINGS


def sample_drilling_stream_app(event: StreamTimeEvent, api: Api, cache: Cache) -> list:

    # You have access to asset_id, company_id and real-time data records from event.
    asset_id = event.asset_id
    company_id = event.company_id
    records = event.records

    # Records is a list
    record_count = len(records)

    # Each element of records has a timestamp
    start_timestamp = records[0].timestamp
    end_timestamp = records[-1].timestamp

    Logger.debug(f"{asset_id=} {company_id=}")
    Logger.debug(f"{start_timestamp=} {end_timestamp=} {record_count=}")

    # Getting last exported timestamp from redis
    last_exported_timestamp = int(cache.load(key='last_exported_timestamp') or 0)

    outputs = []
    for record in records:

        # Making sure we are not processing duplicate data
        if record.timestamp <= last_exported_timestamp:
            continue

        # Each element of records has data
        weight_on_bit = record.data.get("weight_on_bit", 0)
        hook_load = record.data.get("hook_load", 0)

        output = {
            "timestamp": record.timestamp,
            "asset_id": asset_id,
            "company_id": company_id,
            "provider": SETTINGS.provider,
            "collection": SETTINGS.output_collection,
            "data": {
                "hook_load": hook_load,
                "weight_on_bit": weight_on_bit,
                "wob_plus_hkld": weight_on_bit + hook_load
            },
            "version": SETTINGS.version
        }

        outputs.append(output)

    if outputs:
        # if request fails, lambda will be reinvoked. so no exception handling
        Logger.debug(f"{outputs=}")
        api.post(
            f"api/v1/data/{SETTINGS.provider}/{SETTINGS.output_collection}/", data=outputs,
        ).raise_for_status()

        # Storing the last timestamp of the output to cache
        cache.store(key='last_exported_timestamp', value=outputs[-1].get("timestamp"))

    return outputs
