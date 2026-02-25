import unittest.mock

from corva import Api, ScheduledDepthEvent, Cache

from lambda_function import lambda_handler


# Since we do not have a localhost API running we will mock our API requests with the help of unittest.mock built-in python library. 

# Sending a ScheduledDataTimeEvent with the required params for the app.  
# API get_dataset & api.post calls are mocked here.

def test_app(app_runner, cache: Cache):
    event = ScheduledDepthEvent(
        company_id=1, asset_id=1234, log_identifier='1', interval=1, top_depth=1, bottom_depth=2
    )

    with unittest.mock.patch.object(
        Api, 'get_dataset', return_value=[{'data': {'rop': 5}, 'company_id': 1, 'measured_depth': 2}]
    ), unittest.mock.patch.object(Api, 'post') as post_patch:
        app_runner(lambda_handler, event=event)

    # Testing the output data structure & values here
    assert post_patch.call_args.kwargs['data'] == [{
        "measured_depth": 2.0,
        "asset_id": 1234,
        "company_id": 1,
        "log_identifier": '1',
        "provider": 'test-provider',
        "collection": 'example-scheduled-depth-app',
        "data": {
            "mean_rop": 5,
            "top_depth": 1,
            "bottom_depth": 2
        },
        "version": 1
    }]
    
