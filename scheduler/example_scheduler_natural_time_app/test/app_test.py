from corva import ScheduledDataTimeEvent
from lambda_function import lambda_handler


def test_app(app_runner):
    event = ScheduledDataTimeEvent(
        company_id=1, asset_id=1234, start_time=1578291000, end_time=1578291300
    )

    app_runner(lambda_handler, event=event)
