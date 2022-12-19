import unittest.mock

from corva import Api, ScheduledDataTimeEvent

from lambda_function import lambda_handler


def test_app(app_runner):
    event = ScheduledDataTimeEvent(
        company_id=1, asset_id=1234, start_time=1578291000, end_time=1578291300
    )

    with unittest.mock.patch.object(
        Api, 'get_dataset', return_value=[{'data': {'rop': 15}, 'company_id': 1}]
    ), unittest.mock.patch.object(Api, 'post') as post_patch:
        app_runner(lambda_handler, event=event)

    assert post_patch.call_args.kwargs['data'] == [
        {
            'timestamp': 1578291300,
            'asset_id': 1234,
            'company_id': 1,
            'provider': 'test-provider',
            'collection': 'example-scheduled-data-time-app',
            'data': {'rop': 15, 'start_time': 1578291000, 'end_time': 1578291300},
            'version': 1,
        }
    ]
