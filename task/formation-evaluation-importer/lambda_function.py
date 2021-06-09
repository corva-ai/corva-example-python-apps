from corva import Api, TaskEvent, task

from src.app import formation_evaluation_importer


@task
def lambda_handler(event: TaskEvent, api: Api) -> None:
    formation_evaluation_importer(event=event, api=api)
