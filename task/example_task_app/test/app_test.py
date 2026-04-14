import unittest.mock

from corva import Api, TaskEvent

from lambda_function import lambda_handler

# Since we do not have a localhost API running we will mock our API requests with the help of unittest.mock built-in python library. 

# Sending a TaskEvent with the required params for the app.  
# API api.post call is mocked here.

def test_app(app_runner):
    event = TaskEvent(
        company_id=1,
        asset_id=1234,
        properties={
            "timestamp": 1578291300,
            "discounted_revenue": 120,
            "discounted_operating_costs": 20,
            "drilling_and_completions_costs": 30,
        },
    )

    with unittest.mock.patch.object(Api, 'post') as post_patch:
        app_runner(lambda_handler, event=event)
    
    # Testing the output data structure & values here
    assert post_patch.call_args.kwargs['data'] == [
       {
            "asset_id": 1234,
            "company_id": 1,
            "provider": "test-provider",
                "collection": "example-task-app",
            "data": {
                "discounted_revenue": 120,
                "discounted_operating_costs": 20,
                "drilling_and_completions_costs": 30,
                "npv": 70,
            },
            "version": 1,
            "timestamp": 1578291300
        }
    ]
