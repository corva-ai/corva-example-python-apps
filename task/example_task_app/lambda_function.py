from corva import Api, Logger, TaskEvent, task
from src.app import example_task_app

@task
def lambda_handler(event: TaskEvent, api: Api):
    """Insert your logic here"""
    Logger.info('This is my task app log!')
    return example_task_app(event, api)


