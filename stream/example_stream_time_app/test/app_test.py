from corva import StreamTimeEvent, StreamTimeRecord
from lambda_function import lambda_handler


def test_app(app_runner):
    event = StreamTimeEvent(
        company_id=1,
        asset_id=1234,
        records=[
            StreamTimeRecord(
                data={'bit_depth': 4980, 'hole_depth': 5000}, timestamp=1620905165
            )
        ],
    )

    app_runner(lambda_handler, event=event)
