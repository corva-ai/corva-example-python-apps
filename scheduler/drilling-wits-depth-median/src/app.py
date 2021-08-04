from typing import Dict

from corva import Api, Cache, Logger, ScheduledDepthEvent, scheduled

from src import api as api_lib
from src import models, service
from src.configuration import SETTINGS


@scheduled
def app(event: ScheduledDepthEvent, api: Api, cache: Cache):
    records = api_lib.get_records(
        api=api,
        asset_id=event.asset_id,
        log_identifier=event.log_identifier,
        top_depth=event.top_depth,
        bottom_depth=event.bottom_depth,
    )

    if not records:
        Logger.info('Fetched zero records. Returning early.')
        return

    milestone_to_records = service.group_records_by_milestone(
        records=records, interval=event.interval
    )

    out_dataset = SETTINGS.OUT_DATASET.format(interval=event.interval)

    existing_summaries = api_lib.get_summaries(
        api=api,
        asset_id=event.asset_id,
        log_identifier=event.log_identifier,
        measured_depths=list(milestone_to_records),
        collection=out_dataset,
    )

    measured_depth_to_existing_summary: Dict[float, models.InSummary] = {
        summary.measured_depth: summary for summary in existing_summaries
    }

    new_summaries = []
    for milestone, records in milestone_to_records.items():
        stats = service.get_stats(data=[record.data for record in records])

        if existing_summary := measured_depth_to_existing_summary.get(milestone):
            api_lib.update_data(
                api=api,
                collection=out_dataset,
                id_=existing_summary.id,
                data=models.UpdateSummary(data=stats).dict(),
            )
            continue

        new_summaries.append(
            models.Summary(
                asset_id=event.asset_id,
                version=SETTINGS.VERSION,
                measured_depth=milestone,
                log_identifier=event.log_identifier,
                data=stats,
            )
        )

    if new_summaries:
        api_lib.save_data(
            api=api,
            collection=out_dataset,
            data=[summary.dict() for summary in new_summaries],
        )
