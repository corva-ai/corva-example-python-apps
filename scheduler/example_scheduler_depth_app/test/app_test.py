import unittest.mock

from corva import Api, ScheduledDepthEvent, Cache

from lambda_function import lambda_handler


def test_app(app_runner, cache: Cache):
    event = ScheduledDepthEvent(
        company_id=1, asset_id=1234, log_identifier=1, interval=1, top_depth=1, bottom_depth=2
    )

    with unittest.mock.patch.object(
        Api, 'get_dataset', return_value=[{'data': {'dep': 5}, 'company_id': 1, }]
    ), unittest.mock.patch.object(Api, 'post') as post_patch:
        app_runner(lambda_handler, event=event)

    assert post_patch.call_args.kwargs['data'] == [{
        "measured_depth": 2.0,
        "asset_id": 1234,
        "company_id": 1,
        "log_identifier": '1',
        "provider": 'test-provider',
        "collection": 'example-scheduled-depth-app',
        "data": {"dep": 5},
        "version": 1
    }]
    
