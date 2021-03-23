from corva import Corva

from src.app import formation_evaluation_importer


def lambda_handler(event, context):
    corva = Corva(context)
    corva.task(fn=formation_evaluation_importer, event=event)
