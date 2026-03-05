import unittest.mock

from corva import Api, StreamDepthEvent

from lambda_function import lambda_handler

# Since we do not have a localhost API running we will mock our API requests with the help of unittest.mock built-in python library. 

# Sending a StreamDepthEvent with the required params for the app.  
# API api.post call is mocked here.

def test_app(app_runner):
    # prepare a single record with measured_depth and spare data for the app logic
    event = StreamDepthEvent(
        company_id=1,
        asset_id=1234,
        log_identifier="5701c048cf9a",
        records=[
            {
                "measured_depth": 1000.0,
                "data": {"sppa": 100, "tflo": 200},
            }
        ],
    )

    with unittest.mock.patch.object(Api, 'post') as post_patch:
        app_runner(lambda_handler, event=event)
    
    # Testing the output data structure & values here (hp = (100 * 200) / 1714)
    expected_hp = (100 * 200) / 1714
    assert post_patch.call_args.kwargs['data'] == [
        {
            "measured_depth": 1000.0,
            "log_identifier": "5701c048cf9a",
            "company_id": 1,
            "asset_id": 1234,
            "version": 1,
            "provider": "test-provider",
            "collection": "example-stream-depth-app",
            "data": {
                "standpipe_pressure": 100,
                "flow_rate": 200,
                "pump_hydraulic_horsepower": expected_hp,
            },
        }
    ]
