import unittest.mock

from corva import Api, StreamTimeEvent

from lambda_function import lambda_handler

# Since we do not have a localhost API running we will mock our API requests with the help of unittest.mock built-in python library. 

# Sending a StreamTimeEvent with the required params for the app.  
# API api.post call is mocked here.

def test_app(app_runner):
    event = StreamTimeEvent(
        company_id=1, asset_id=1234, records=[{"timestamp": 1578291300, "data": {"pump_spm_1": 25, "pump_spm_2": 30}}]
    )

    with unittest.mock.patch.object(Api, 'post') as post_patch:
        app_runner(lambda_handler, event=event)
    
    # Testing the output data structure & values here
    assert post_patch.call_args.kwargs['data'] == [
       {
            "timestamp": 1578291300,
            "asset_id": 1234,
            "company_id": 1,
            "provider": "test-provider",
            "collection": "example-stream-time-app",
            "data": {
                "pump_spm_1": 25,
                "pump_spm_2": 30
            },
            "version": 1,
        }
    ]
