import datetime
import inspect
import io
import re

import freezegun
import lasio
import pytest
import requests
import requests_mock as requests_mock_lib
from corva import TaskEvent
from pytest_mock import MockerFixture
from requests_mock import Mocker as RequestsMocker

from lambda_function import lambda_handler
from src.configuration import SETTINGS
from src.logger import LOGGER
from src.models import (
    EventProperties,
    FormationEvaluationData,
    FormationEvaluationDataMetadata,
    FormationEvaluationMetadata,
    FormationEvaluationMetadataData,
    LasSectionRowData,
    LasSectionRowMapping,
    ParsedLasSectionRow,
)

LAS_V_1_2 = inspect.cleandoc(
    """
    ~Version ---------------------------------------------------
    VERS.   1.2 : CWLS LOG ASCII STANDARD - VERSION 1.2
    WRAP.    NO : One line per depth step
    DLM . SPACE : Column Data Section Delimiter
    ~Well ------------------------------------------------------
    DEPT  .FEET   :
    WELL  .WELL   :
    ~Curve Information -----------------------------------------
    DEPT  .FEET   :
    CURVE .CURVE  :
    ~Params ----------------------------------------------------
    DEPT  .FEET   :
    PARAM .PARAM  :
    ~Other -----------------------------------------------------
    Other data.
    ~ASCII -----------------------------------------------------
        1.00000    4.00000
        2.00000    5.00000
        3.00000    6.00000
    """
)

LAS_V_2_0 = inspect.cleandoc(
    """
    ~Version ---------------------------------------------------
    VERS.   2.0 : CWLS log ASCII Standard -VERSION 2.0
    WRAP.    NO : One line per depth step
    DLM . SPACE : Column Data Section Delimiter
    ~Well ------------------------------------------------------
    DEPT  .FEET    :
    WELL  .WELL    :
    ~Curve Information -----------------------------------------
    DEPT  .FEET    :
    CURVE .CURVE   :
    ~Params ----------------------------------------------------
    DEPT  .FEET    :
    PARAM .PARAM   :
    ~Other -----------------------------------------------------
    Other data.
    ~ASCII -----------------------------------------------------
        1.00000    4.00000
        2.00000    5.00000
        3.00000    6.00000
    """
)


@pytest.mark.parametrize(
    'matcher',
    (
        re.compile(r'v1/data/.+/.+\.data/'),
        re.compile(r'v1/data/.+/.+\.metadata/'),
    ),
)
def test_behavior_if_delete_data_fails(
    matcher,
    app_runner,
    requests_mock: RequestsMocker,
    mocker: MockerFixture,
):
    """App logs the error message and continues the execution."""

    properties = EventProperties(file_name='file/name', file_url='https://localhost')
    event = TaskEvent(
        asset_id=0,
        company_id=0,
        properties=properties.dict(by_alias=True),
    )

    requests_mock.delete(requests_mock_lib.ANY, status_code=200)
    requests_mock.delete(matcher, status_code=400)
    # return early by patching get_file to raise
    logger_spy = mocker.spy(LOGGER, 'error')
    get_file_patch = mocker.patch('src.app.get_file', side_effect=Exception)

    pytest.raises(Exception, app_runner, lambda_handler, event)

    logger_spy.assert_called_once_with(
        f'Could not delete file_name={properties.file_name} '
        f'for asset_id={event.asset_id}.'
    )
    get_file_patch.assert_called_once()


@pytest.mark.parametrize(
    'curve,exc_ctx',
    (
        [
            ('dept', [1]),
            pytest.raises(Exception, match=''),
        ],
        [
            ('depth', [1]),
            pytest.raises(Exception, match=''),
        ],
        [
            ('random', [1]),
            pytest.raises(Exception, match=r'The index curve must be depth\.'),
        ],
    ),
)
def test_validate_index_curve_mnemonic(
    curve: tuple, exc_ctx, mocker: MockerFixture, app_runner
):
    event = TaskEvent(
        asset_id=0,
        company_id=0,
        properties=EventProperties(
            file_name='file/name', file_url='https://localhost'
        ).dict(by_alias=True),
    )

    las_file = lasio.LASFile()
    las_file.add_curve(*curve)

    out = io.StringIO()
    las_file.write(out)

    mocker.patch('src.app.delete_data_by_file_name')
    mocker.patch('src.app.get_file', return_value=out.getvalue())
    # return early by throwing an exception
    mocker.patch.object(
        lasio.LASFile, 'data', new_callable=mocker.PropertyMock, side_effect=Exception
    )

    with exc_ctx:
        app_runner(lambda_handler, event)


@pytest.mark.parametrize('las_file', (LAS_V_1_2, LAS_V_2_0))
def test_correct_formation_evaluation_metadata(
    las_file,
    mocker: MockerFixture,
    app_runner,
    requests_mock: RequestsMocker,
):
    properties = EventProperties(file_name='file/name', file_url='https://localhost')
    event = TaskEvent(
        asset_id=0,
        company_id=0,
        properties=properties.dict(by_alias=True),
    )

    mocker.patch('src.app.delete_data_by_file_name')
    mocker.patch('src.app.get_file', return_value=las_file)
    # return early by throwing exception
    post_mock = requests_mock.post(
        re.compile(r'v1/data/.+/.+\.metadata/'), status_code=400
    )

    # freeze time with non-zero tz_offset to validate,
    # that timestamp is takes from timezone aware datetime instance.
    with freezegun.freeze_time("2021-03-26 00:00:00", tz_offset=1):
        timestamp = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp())
        pytest.raises(Exception, app_runner, lambda_handler, event)

    # expected data

    # curves
    expected_curves = [
        ParsedLasSectionRow(
            data=LasSectionRowData.construct(
                mnemonic='dept',
                units='FEET',
                value='',
                descr='',
            ),
            mapping=LasSectionRowMapping.construct(
                mnemonic='md', unit='ft', bucket='Length'
            ),
        ),
        ParsedLasSectionRow(
            data=LasSectionRowData.construct(
                mnemonic='curve',
                units='CURVE',
                value='',
                descr='',
            ),
            mapping=LasSectionRowMapping.construct(
                mnemonic='curve',
                unit='CURVE',
                bucket='Other',
            ),
        ),
    ]

    # params
    expected_params = [
        ParsedLasSectionRow(
            data=LasSectionRowData.construct(
                mnemonic='dept',
                units='FEET',
                value='',
                descr='',
            ),
            mapping=LasSectionRowMapping.construct(
                mnemonic='md', unit='ft', bucket='Length'
            ),
        ),
        ParsedLasSectionRow(
            data=LasSectionRowData.construct(
                mnemonic='param',
                units='PARAM',
                value='',
                descr='',
            ),
            mapping=LasSectionRowMapping.construct(
                mnemonic='param',
                unit='PARAM',
                bucket='Other',
            ),
        ),
    ]

    # well
    expected_wells = [
        ParsedLasSectionRow(
            data=LasSectionRowData.construct(
                mnemonic='dept',
                units='FEET',
                value='',
                descr='',
            ),
            mapping=LasSectionRowMapping.construct(
                mnemonic='md',
                unit='ft',
                bucket='Length',
            ),
        ),
        ParsedLasSectionRow(
            data=LasSectionRowData.construct(
                mnemonic='well',
                units='WELL',
                value='',
                descr='',
            ),
            mapping=LasSectionRowMapping.construct(
                mnemonic='well',
                unit='WELL',
                bucket='Other',
            ),
        ),
    ]

    expected_fem = FormationEvaluationMetadata(
        asset_id=event.asset_id,
        timestamp=timestamp,
        company_id=event.company_id,
        collection=SETTINGS.metadata_collection,
        app=SETTINGS.app_name,
        provider=SETTINGS.provider,
        data=FormationEvaluationMetadataData(
            params=expected_params,
            well=expected_wells,
            curve=expected_curves,
            other='Other data.',
        ),
        file_name=properties.file_name,
        records_count=3,
        version=SETTINGS.version,
    )

    assert post_mock.last_request.json() == [expected_fem.dict()]


def test_fail_if_could_not_save_metadata(
    mocker: MockerFixture,
    app_runner,
    requests_mock: RequestsMocker,
):
    event = TaskEvent(
        asset_id=0,
        company_id=0,
        properties=EventProperties(
            file_name='file/name', file_url='https://localhost'
        ).dict(by_alias=True),
    )

    mocker.patch('src.app.delete_data_by_file_name')
    mocker.patch(
        'src.app.get_file',
        return_value=LAS_V_2_0,
    )
    requests_mock.post(re.compile(r'v1/data/.+/.+\.metadata/'), status_code=400)

    pytest.raises(
        Exception, app_runner, lambda_handler, event, match=r'Could not save the data\.'
    )


@pytest.mark.parametrize('las_file', (LAS_V_1_2, LAS_V_2_0))
def test_correct_formation_evaluation_data(
    las_file,
    mocker: MockerFixture,
    app_runner,
    requests_mock: RequestsMocker,
):
    properties = EventProperties(file_name='file/name', file_url='https://localhost')
    event = TaskEvent(
        asset_id=0,
        company_id=0,
        properties=properties.dict(by_alias=True),
    )

    mocker.patch('src.app.delete_data_by_file_name')
    mocker.patch('src.app.get_file', return_value=las_file)
    requests_mock.post(
        re.compile(r'v1/data/.+/.+\.metadata/'),
        status_code=200,
        json={'inserted_ids': ['0']},
    )
    post_mock = requests_mock.post(
        re.compile(r'v1/data/.+/.+\.data/'),
        status_code=200,
        json={'inserted_ids': ['0']},
    )
    mocker.patch.object(SETTINGS, 'chunk_size', 2)

    # freeze time with non-zero tz_offset to validate,
    # that timestamp is takes from timezone aware datetime instance.
    with freezegun.freeze_time("2021-03-26 00:00:00", tz_offset=1):
        timestamp = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp())
        app_runner(lambda_handler, event)

    excpected_feds = [
        FormationEvaluationData(
            asset_id=event.asset_id,
            timestamp=timestamp,
            company_id=event.company_id,
            collection=SETTINGS.data_collection,
            app=SETTINGS.app_name,
            provider=SETTINGS.provider,
            metadata=FormationEvaluationDataMetadata(
                formation_evaluation_id='0', file_name=properties.file_name
            ),
            data=data,
            version=SETTINGS.version,
        ).dict()
        for data in [
            {'md': 1, 'curve': 4},
            {'md': 2, 'curve': 5},
            {'md': 3, 'curve': 6},
        ]
    ]

    assert len(post_mock.request_history) == 2
    assert post_mock.request_history[0].json() == excpected_feds[:2]
    assert post_mock.request_history[1].json() == excpected_feds[2:]


def test_fail_if_could_not_save_data(
    mocker: MockerFixture,
    app_runner,
    requests_mock: RequestsMocker,
):
    event = TaskEvent(
        asset_id=0,
        company_id=0,
        properties=EventProperties(
            file_name='file/name', file_url='https://localhost'
        ).dict(by_alias=True),
    )

    mocker.patch('src.app.delete_data_by_file_name')
    mocker.patch(
        'src.app.get_file',
        return_value=LAS_V_2_0,
    )
    requests_mock.post(
        re.compile(r'v1/data/.+/.+\.metadata/'),
        status_code=200,
        json={'inserted_ids': ['0']},
    )
    requests_mock.post(re.compile(r'v1/data/.+/.+\.data/'), status_code=400)

    pytest.raises(requests.HTTPError, app_runner, lambda_handler, event)
