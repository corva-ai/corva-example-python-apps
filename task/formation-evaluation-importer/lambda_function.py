from corva import Corva

from src.app import formation_evaluation_importer


def lambda_handler(event, context):
    Corva(context).task(fn=formation_evaluation_importer, event=event)
