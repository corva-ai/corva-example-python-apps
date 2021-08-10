from typing import List

import pytest
from corva import Api, ScheduledDepthEvent
from pytest_mock import MockerFixture

from src import models, network, service
from src.app import app


def test_app_returns_early(app_runner, mocker: MockerFixture):
    """It returns early if no records were fetched from the API."""

    event = ScheduledDepthEvent(
        asset_id=0,
        company_id=0,
        top_depth=0.0,
        bottom_depth=0.0,
        log_identifier='',
        interval=0.0,
    )

    mocker.patch.object(network, 'get_records', return_value=[])
    spy = mocker.spy(service, 'group_records_by_milestone')

    app_runner(app, event)

    spy.assert_not_called()


def test_app_groups_records_by_depth_milestone(app_runner, mocker: MockerFixture):
    """
    It groups fetched records by depth milestone and calculates stats for each group
    separately.
    """

    event = ScheduledDepthEvent(
        asset_id=0,
        company_id=0,
        top_depth=0.0,
        bottom_depth=0.0,
        log_identifier='',
        interval=3.0,
    )

    records = [
        models.Record(measured_depth=0.0, data={'key': 0}).dict(),  # group 1
        models.Record(measured_depth=1.50, data={'key': 1}).dict(),  # group 1
        models.Record(measured_depth=1.51, data={'key': 2}).dict(),  # group 2
        models.Record(measured_depth=3.0, data={'key': 3}).dict(),  # group 2
        models.Record(measured_depth=4.50, data={'key': 4}).dict(),  # group 2
    ]

    mocker.patch.object(Api, 'get_dataset', side_effect=[records, []])
    post_mock = mocker.patch.object(Api, 'post')

    app_runner(app, event)

    expected = [
        models.Summary(
            asset_id=0,
            version=1,
            measured_depth=0.0,
            log_identifier='',
            data={'key': 0.5},
        ).dict(),
        models.Summary(
            asset_id=0,
            version=1,
            measured_depth=3.0,
            log_identifier='',
            data={'key': 3},
        ).dict(),
    ]

    post_mock.assert_called_once_with(
        'api/v1/data/test-provider/drilling.wits.depth.median-3.0ft/', data=expected
    )


def test_app_updates_existing_summary(app_runner, mocker: MockerFixture):
    """It fetches existing summary and updates it, without creating a new one."""

    event = ScheduledDepthEvent(
        asset_id=0,
        company_id=0,
        top_depth=0.0,
        bottom_depth=0.0,
        log_identifier='',
        interval=3.0,
    )

    records = [models.Record(measured_depth=0.0, data={'key': 0}).dict()]
    in_summaries = [models.InSummary(_id='123', measured_depth=0.0).dict(by_alias=True)]

    mocker.patch.object(Api, 'get_dataset', side_effect=[records, in_summaries])
    post_mock = mocker.patch.object(Api, 'post')
    patch_mock = mocker.patch.object(Api, 'patch')

    app_runner(app, event)

    expected = models.UpdateSummary(data={'key': 0.0}).dict()

    post_mock.assert_not_called()
    patch_mock.assert_called_once_with(
        'api/v1/data/test-provider/drilling.wits.depth.median-3.0ft/123/', data=expected
    )


def test_app_creates_new_summary(app_runner, mocker: MockerFixture):
    """It creates a new summary if could not fetch an existing one."""

    event = ScheduledDepthEvent(
        asset_id=0,
        company_id=0,
        top_depth=0.0,
        bottom_depth=0.0,
        log_identifier='',
        interval=3.0,
    )

    records = [models.Record(measured_depth=0.0, data={'key': 0}).dict()]

    mocker.patch.object(Api, 'get_dataset', side_effect=[records, []])
    post_mock = mocker.patch.object(Api, 'post')

    app_runner(app, event)

    expected = [
        models.Summary(
            asset_id=event.asset_id,
            version=1,
            measured_depth=0.0,
            log_identifier=event.log_identifier,
            data={'key': 0.0},
        ).dict()
    ]

    post_mock.assert_called_once_with(
        'api/v1/data/test-provider/drilling.wits.depth.median-3.0ft/', data=expected
    )


def test_app_succeeds_if_no_new_summary(app_runner, mocker: MockerFixture):
    """
    It succeeds if there are no new summaries to create. This could happen if all
    summaries existed before.
    """

    event = ScheduledDepthEvent(
        asset_id=0,
        company_id=0,
        top_depth=0.0,
        bottom_depth=0.0,
        log_identifier='',
        interval=3.0,
    )

    records = [models.Record(measured_depth=0.0, data={'key': 0}).dict()]
    in_summaries = [models.InSummary(_id='123', measured_depth=0.0).dict(by_alias=True)]

    mocker.patch.object(Api, 'get_dataset', side_effect=[records, in_summaries])
    post_mock = mocker.patch.object(Api, 'post')
    patch_mock = mocker.patch.object(Api, 'patch')

    app_runner(app, event)

    post_mock.assert_not_called()
    patch_mock.assert_called_once()


@pytest.mark.parametrize(
    'record_data,expected_stats',
    (
        [[{'key1': float(0.0)}, {'key1': int(1)}, {'key1': '0'}], {'key1': 0.5}],
        [
            [{'key1': 0}, {'key1': 1.0}],
            {'key1': 0.5},
        ],
        [
            [{'key1': [0, 2]}, {'key1': [1.0, 3.0]}],
            {'key1': [0.5, 2.5]},
        ],
        [[{'key1': 0}, {}], {'key1': 0}],
        [[{'key1': '0'}], {}],
        [[{'key1': [0]}, {'key1': [1, 2]}], {'key1': [0.5, 2]}],
        [[{'key1': ['0']}], {}],
        [[{'key1': [0, '0']}, {'key1': [1, '1']}], {'key1': [0.5, None]}],
    ),
    ids=[
        'bad values are filtered',
        '[single] median for single value',
        '[array] median for array value',
        '[single] records have different sets of keys',
        '[single] no median, because key has bad value',
        '[array] array has rows with different length',
        '[array] all medians are None',
        '[array] mix of None and proper medians',
    ],
)
def test_app_stats(
    record_data: List[dict], expected_stats: dict, app_runner, mocker: MockerFixture
):
    """It returns correct stats in different cases."""

    event = ScheduledDepthEvent(
        asset_id=0,
        company_id=0,
        top_depth=0.0,
        bottom_depth=0.0,
        log_identifier='',
        interval=3.0,
    )

    records = [
        models.Record(measured_depth=0.0, data=datum).dict() for datum in record_data
    ]

    mocker.patch.object(Api, 'get_dataset', side_effect=[records, []])
    post_mock = mocker.patch.object(Api, 'post')

    app_runner(app, event)

    assert post_mock.call_args.kwargs['data'][0]['data'] == expected_stats
