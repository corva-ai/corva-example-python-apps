from corva import Api, Logger, TaskEvent

from src.configuration import SETTINGS


def example_task_app(event: TaskEvent, api: Api) -> list:

    # You have access to asset_id, company_id and real-time data records from event.
    asset_id = event.asset_id
    company_id = event.company_id
    timestamp = event.properties["timestamp"]

    Logger.debug(f"{asset_id=} {company_id=}")

    # Aggregating everything into one json object. I have also added some custom fields for testing purposes.
    output = {
        "asset_id": asset_id,
        "company_id": company_id,
        "provider": SETTINGS.provider,
        "collection": SETTINGS.output_collection,
        "data": {
            "my_field": "test_value",
            "my_new_field": "my_new_test_value"
        },
        "version": SETTINGS.version,
        "timestamp": timestamp
    }
    
    Logger.debug({f"{output=}"})

    # Checking if any data is present and then seding a POST request to Corva Data API. Please note that data is always a list.
    if output:
        # if request fails, lambda will be reinvoked. so no exception handling
        api.post(
            f"api/v1/data/{SETTINGS.provider}/{SETTINGS.output_collection}/", data=[output],
        ).raise_for_status()
    return output