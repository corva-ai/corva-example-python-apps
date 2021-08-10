from typing import List

import pydantic
from corva import Api

from src import models
from src.configuration import SETTINGS


def get_records(
    api: Api,
    asset_id: int,
    log_identifier: str,
    top_depth: float,
    bottom_depth: float,
) -> List[models.Record]:
    raw_records = api.get_dataset(
        provider='corva',
        dataset=SETTINGS.IN_DATASET,
        query={
            'asset_id': asset_id,
            'log_identifier': log_identifier,
            'measured_depth': {'$gt': top_depth, '$lte': bottom_depth},
        },
        sort={'measured_depth': 1},
        limit=SETTINGS.API_LIMIT,
        fields='measured_depth,data',
    )

    return pydantic.parse_obj_as(List[models.Record], raw_records)


def get_summaries(
    api: Api,
    asset_id: int,
    log_identifier: str,
    measured_depths: List[float],
    collection: str,
) -> List[models.InSummary]:
    raw_summaries = api.get_dataset(
        provider=SETTINGS.PROVIDER,
        dataset=collection,
        query={
            'asset_id': asset_id,
            'log_identifier': log_identifier,
            'measured_depth': {'$in': measured_depths},
        },
        sort={'measured_depth': 1},
        limit=len(measured_depths),
        fields='_id,measured_depth',
    )

    return pydantic.parse_obj_as(List[models.InSummary], raw_summaries)


def save_data(
    api: Api,
    collection: str,
    data: List[dict],
) -> None:
    api.post(
        f'api/v1/data/{SETTINGS.PROVIDER}/{collection}/',
        data=data,
    ).raise_for_status()


def update_data(
    api: Api,
    collection: str,
    id_: str,
    data: dict,
) -> None:
    api.patch(
        f'api/v1/data/{SETTINGS.PROVIDER}/{collection}/{id_}/', data=data
    ).raise_for_status()
