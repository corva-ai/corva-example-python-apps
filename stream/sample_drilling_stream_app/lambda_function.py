from corva import Corva

from src.app import sample_drilling_stream_app


def lambda_handler(event, context):
    """The main entry point of the AWS Lambda function"""

    return Corva(context).stream(sample_drilling_stream_app, event)
