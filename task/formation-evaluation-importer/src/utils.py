import json
import logging
import urllib.request
from typing import Iterator, List

import requests
from corva import Api

from src.models import SaveDataReponse

LOGGER = logging.getLogger('main')


def chunker(seq: list, size: int) -> Iterator[list]:
    for idx in range(0, len(seq), size):
        yield seq[idx : idx + size]


def delete_data_by_file_name(
    api: Api, file_name: str, asset_id: int, collection: str, provider: str
) -> None:
    """Deletes data for asset id by file name.

    Raises:
      Exception: if delete was unsuccessful.
    """

    try:
        # delete data first
        api.delete(
            f'v1/data/{provider}/{collection}.data/',
            params={
                'query': json.dumps(
                    {
                        'asset_id': asset_id,
                        'metadata.file': file_name,
                    }
                )
            },
        ).raise_for_status()

        # delete metadata only on data delete success
        api.delete(
            f'v1/data/{provider}/{collection}.metadata/',
            params={'query': json.dumps({'asset_id': asset_id, 'file': file_name})},
        ).raise_for_status()
    except requests.HTTPError as exc:
        raise Exception(f'Could not delete {file_name=} for {asset_id=}.') from exc


def get_file(url: str) -> str:
    with urllib.request.urlopen(url) as file:
        return file.read().decode()


def save_data(
    api: Api, data: List[dict], collection: str, provider: str
) -> SaveDataReponse:
    """Saves the data.

    Raises:
      Exception: if save was unsuccessful.
    """

    response = api.post(f'v1/data/{provider}/{collection}/', data=data)

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise Exception('Could not save the data.') from exc

    return SaveDataReponse(**response.json())
