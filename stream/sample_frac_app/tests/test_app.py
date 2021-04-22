from corva import StreamTimeEvent, StreamTimeRecord

from lambda_function import lambda_handler


def test_stream_time_app(app_runner):
    event = StreamTimeEvent(
        asset_id=0,
        company_id=0,
        records=[
            StreamTimeRecord(timestamp=12345, data={"wellhead_pressure": 141.0, "slurry_flow_rate_in": 0.0, "total_proppant_concentration": 0.0}, stage_number=20),
            StreamTimeRecord(timestamp=12346, data={"wellhead_pressure": 137.0, "slurry_flow_rate_in": 0.0, "total_proppant_concentration": 0.0}, stage_number=20)
        ]
    )

    result = app_runner(fn=lambda_handler, event=event)

    assert result == 20
