from corva import Corva

from src.app import sample_drilling_scheduler_app


def lambda_handler(event, context):
    """The main entry point of the AWS Lambda function"""

    return Corva(context).scheduled(sample_drilling_scheduler_app, event)
