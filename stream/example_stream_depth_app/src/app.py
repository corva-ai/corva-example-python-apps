from corva import Api, Cache, Logger, StreamDepthEvent

from src.configuration import SETTINGS


def example_stream_depth_app(event: StreamDepthEvent, api: Api, cache: Cache):
    
# 3. Here is where you can declare your variables from the argument event: StreamDepthEvent and start using Api, Cache and Logger functionalities. You can obtain key values directly from metadata in the stream app event without making any additional API requests.

    # The stream depth app can declare the following attributes from the StreamDepthEvent: company_id: The company identifier; asset_id: The asset identifier; records: The records that include the indexes and data object with key values. 
    asset_id = event.asset_id
    company_id = event.company_id
    records = event.records
    log_identifier = event.log_identifier

    # Records is a list
    record_count = len(records)

    # Each element of records has a measured_depth. You can declare variables for start and end measured depth. 
    start_measured_depth = records[0].measured_depth
    end_measured_depth = records[-1].measured_depth

    # Utilize the Logger functionality. The default log level is Logger.info. To use a different log level, the log level must be specified in the manifest.json file in the "settings.environment": {"LOG_LEVEL": "DEBUG"}. See the Logger documentation for more information.
    Logger.info(f"{asset_id=} {company_id=}")
    Logger.info(f"{start_measured_depth=} {end_measured_depth=} {record_count=}")

    # Utililize the Cache functionality to get a set key value. The Cache functionality is built on Redis Cache. See the Cache documentation for more information.
    # Getting last exported measured_depth from Cache 
    last_exported_measured_depth = int(float(cache.get(key='last_exported_measured_depth') or 0))

# 4. Here is where you can add your app logic.
    
    # Setting state to append data to an arrray 
    outputs = []
    for record in records:

        # Making sure we are not processing duplicate data
        if record.measured_depth <= last_exported_measured_depth:
            Logger.info(f"Skipping record with measured_depth={record.measured_depth} because it's less than or equal to last_exported_measured_depth={last_exported_measured_depth}")
            continue
        # Each element of records has data. This is how to get specific key values from an embedded object
        standpipe_pressure = record.data.get("sppa", 0)   # psi
        flow_rate = record.data.get("tflo", 0)            # gpm

        # Calculate Pump Hydraulic Horsepower (HHP)
        # HHP = (Pressure * Flow) / 1714
        pump_hydraulic_horsepower = (standpipe_pressure * flow_rate) / 1714 if standpipe_pressure and flow_rate else 0

        # This is how to set up a body of a POST request to store the standpipe_pressure and flow_rate data from the StreamDepthEvent and newly calculated pump_hydraulic_horsepower value 
        output = {
            "measured_depth": record.measured_depth,
            "log_identifier": log_identifier or "Not available",
            "company_id": company_id,
            "asset_id": asset_id,
            "version": SETTINGS.version,
            "provider": SETTINGS.provider,
            "collection": SETTINGS.output_collection,
            "data": {
                "standpipe_pressure": standpipe_pressure,
                "flow_rate": flow_rate,
                "pump_hydraulic_horsepower": pump_hydraulic_horsepower
            },
        }

        # Appending the new data to the empty array
        outputs.append(output)

# 5. Save the newly calculated data in a custom dataset

    # Set up an if statement so that if request fails, lambda will be reinvoked. So no exception handling
    if outputs:
        # Utilize Logger functionality to confirm data in log files
        Logger.debug(f"{outputs=}")

        # Utilize the Api functionality. The data=outputs needs to be an an array because Corva's data is saved as an array of objects. Objects being records. See the Api documentation for more information.
        api.post(
            f"api/v1/data/{SETTINGS.provider}/{SETTINGS.output_collection}/", data=outputs,
        ).raise_for_status()

        # Utililize the Cache functionality to set a key value. The Cache functionality is built on Redis Cache. See the Cache documentation for more information. This example is setting the last measured_depth of the output to Cache
        cache.set(key='last_exported_measured_depth', value=str(outputs[-1].get("measured_depth")))

    return outputs