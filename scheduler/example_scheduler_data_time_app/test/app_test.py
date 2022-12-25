import unittest.mock

from corva import Api, ScheduledDataTimeEvent

from lambda_function import lambda_handler

# Since we do not have a localhost API running we will mock our API requests with the help of unittest.mock built-in python library. 

# Sending a ScheduledDataTimeEvent with the required params for the app.  
# API get_dataset & api.post calls are mocked here.

def test_app(app_runner):
    event = ScheduledDataTimeEvent(
        company_id=1, asset_id=1234, start_time=1578291000, end_time=1578291300
    )

    with unittest.mock.patch.object(
        Api, 'get_dataset', return_value=[{'data': {'rop': 15}, 'company_id': 1}]
    ), unittest.mock.patch.object(Api, 'post') as post_patch:
        app_runner(lambda_handler, event=event)

    # Testing the output data structure & values here
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
