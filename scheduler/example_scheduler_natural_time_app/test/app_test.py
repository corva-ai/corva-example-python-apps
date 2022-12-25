import unittest.mock

from corva import Api, ScheduledNaturalTimeEvent, Cache

from lambda_function import lambda_handler

# Since we do not have a localhost API running we will mock our API requests with the help of unittest.mock built-in python library. 

# Sending a ScheduledNaturalTimeEvent with the required params for the app.  
# API get_dataset & api.post calls are mocked here.

def test_app(app_runner):
    event = ScheduledNaturalTimeEvent(
        company_id=1, asset_id=1234, log_identifier=1, schedule_start=1, interval=1, start_time=12345, end_time=123456
    )

    with unittest.mock.patch.object(
        Api, 'get_dataset', return_value=[{'data': {'weight_on_bit': 5}, 'company_id': 1}]
    ), unittest.mock.patch.object(Api, 'post') as post_patch:
        app_runner(lambda_handler, event=event)

    # Testing the output data structure & values here
    assert post_patch.call_args.kwargs['data'] == [{
        "timestamp": 123456,
        "asset_id": 1234,
        "company_id": 1,
        "provider": "test-provider",
        "collection": "example-scheduled-natural-time-app",
        "data": {
            "mean_weight_on_bit": 5,
            "start_time": 12345,
            "end_time": 123456
        },
        "version": 1
    }]
    
    