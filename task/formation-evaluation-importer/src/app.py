import datetime

from corva import Api, TaskEvent

from src import models, parser, utils
from src.configuration import SETTINGS
from src.logger import LOGGER


def formation_evaluation_importer(event: TaskEvent, api: Api) -> None:
    properties = models.EventProperties.parse_obj(event.properties)

    try:
        # Delete old data. New data will be written to the db as a result of this app.
        utils.delete_data_by_file_name(
            api=api,
            file_name=properties.file_name,
            asset_id=event.asset_id,
            collection=SETTINGS.collection,
            provider=SETTINGS.provider,
        )
    except Exception as exc:
        LOGGER.error(str(exc))

    file = utils.get_file(url=properties.file_url)

    parse_result = parser.parse(file=file)

    timestamp = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp())

    formation_evaluation_metadata = models.FormationEvaluationMetadata(
        asset_id=event.asset_id,
        timestamp=timestamp,
        company_id=event.company_id,
        collection=f'{SETTINGS.collection}.metadata',
        app=SETTINGS.app_name,
        provider=SETTINGS.provider,
        data=models.FormationEvaluationMetadataData(
            params=parse_result.params,
            well=parse_result.well,
            curve=parse_result.curves,
            other=parse_result.other,
        ),
        file_name=properties.file_name,
        records_count=parse_result.n_log_data_rows,
        version=SETTINGS.version,
    )

    # fail in case of exception
    formation_evaluation_metadata_id = utils.save_data(
        api=api,
        data=[formation_evaluation_metadata.dict()],
        collection=f'{SETTINGS.collection}.metadata',
        provider=SETTINGS.provider,
    ).inserted_ids[0]

    for mapped_log_data_chunk in utils.chunker(
        seq=parse_result.mapped_log_data, size=SETTINGS.chunk_size
    ):
        save_datas = []

        for mapped_log_data in mapped_log_data_chunk:
            save_datas.append(
                models.FormationEvaluationData(
                    asset_id=event.asset_id,
                    timestamp=timestamp,
                    company_id=event.company_id,
                    collection=f'{SETTINGS.collection}.data',
                    app=SETTINGS.app_name,
                    provider=SETTINGS.provider,
                    metadata=models.FormationEvaluationDataMetadata(
                        formation_evaluation_id=formation_evaluation_metadata_id,
                        file_name=properties.file_name,
                    ),
                    data=mapped_log_data,
                    version=SETTINGS.version,
                ).dict()
            )

        utils.save_data(
            api=api,
            data=save_datas,
            collection=f'{SETTINGS.collection}.data',
            provider=SETTINGS.provider,
        )
