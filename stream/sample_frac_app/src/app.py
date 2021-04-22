from corva import Api, Cache, Logger, StreamTimeEvent


def sample_frac_stream_app(event: StreamTimeEvent, api: Api, cache: Cache):

    # You have access to asset_id, company_id and real-time data records from event.
    asset_id = event.asset_id
    company_id = event.company_id
    records = event.records

    # Records is a list
    record_count = len(records)

    # Each element of records has timestamp and data
    start_timestamp = records[0].timestamp
    end_timestamp = records[-1].timestamp

    # This is just data from the first element of the records list. Each record will have it's associated data
    data = records[0].data

    # This is stage_number from the first element of the records list. Each record will have it's stage_number
    stage_number = records[0].stage_number

    # data is a dictionary with all the real-time channels corresponding to that timestamp
    real_time_channels = list(data.keys())

    Logger.info(f"asset_id {asset_id} company_id {company_id} stage_number {stage_number}")
    Logger.info(f"start_timestamp {start_timestamp} end_timestamp {end_timestamp} record_count {record_count}")
    Logger.info(f"real-time channels {real_time_channels}")

    return stage_number
