import requests_mock as requests_mock_lib
from corva import StreamTimeEvent, StreamTimeRecord

from lambda_function import lambda_handler


def test_stream_time_app(app_runner, requests_mock: requests_mock_lib.Mocker):
    event = StreamTimeEvent(
        asset_id=0,
        company_id=0,
        records=[
            StreamTimeRecord(timestamp=12345, data={"hook_load": 141.0, "weight_on_bit": 5.0}),
            StreamTimeRecord(timestamp=12346, data={"hook_load": 137.0, "weight_on_bit": 8.0})
        ]
    )
    expected_result = [
        {
            'asset_id': 0,
            'collection': 'sample-drilling-stream-collection',
            'company_id': 0,
            'data': {
                'hook_load': 141.0,
                'weight_on_bit': 5.0,
                'wob_plus_hkld': 146.0
            },
            'provider': 'test-provider',
            'timestamp': 12345,
            'version': 1
        },
        {
            'asset_id': 0,
            'collection': 'sample-drilling-stream-collection',
            'company_id': 0,
            'data': {
                'hook_load': 137.0,
                'weight_on_bit': 8.0,
                'wob_plus_hkld': 145.0
            },
            'provider': 'test-provider',
            'timestamp': 12346,
            'version': 1
        }
    ]

    requests_mock.post(requests_mock_lib.ANY, status_code=201)
    result = app_runner(fn=lambda_handler, event=event)

    assert result == expected_result
