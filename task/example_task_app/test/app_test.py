import unittest.mock

from corva import Api, TaskEvent

from lambda_function import lambda_handler


def test_app(app_runner):
    event = TaskEvent(
        company_id=1, asset_id=1234, properties={"timestamp": 1578291300}
    )

    with unittest.mock.patch.object(Api, 'post') as post_patch:
        app_runner(lambda_handler, event=event)

    assert post_patch.call_args.kwargs['data'] == [
       {
            "asset_id": 1234,
            "company_id": 1,
            "provider": "test-provider",
            "collection": "example-task-app",
            "data": {
                "my_field": "test_value",
                "my_new_field": "my_new_test_value"
            },
            "version": 1,
            "timestamp": 1578291300
        }
    ]
