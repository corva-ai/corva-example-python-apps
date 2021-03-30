from typing import Dict, List, Union

import lasio

from src.constants import MNEMONICS, UNIT_BUCKETS, UNITS
from src.models import (
    ParsedLasFile,
    ParsedLasSectionRow,
    LasSectionRowData,
    LasSectionRowMapping,
)


def parse_section_row(
    row: lasio.HeaderItem,
    units: Dict[str, str],
    unit_buckets: Dict[str, str],
    mnemonics: Dict[str, str],
) -> ParsedLasSectionRow:
    unit_name = row.unit.lower()
    mnemonic_name = row.mnemonic.lower()

    data = LasSectionRowData(
        mnemonic=row.mnemonic, units=row.unit, value=row.value, descr=row.descr
    )
    mapping = LasSectionRowMapping(
        mnemonic=mnemonics.get(mnemonic_name, row.mnemonic),
        unit=units.get(unit_name, row.unit),
        bucket=unit_buckets.get(unit_name, unit_buckets['*']),
    )

    return ParsedLasSectionRow(data=data, mapping=mapping)


def parse_section(
    section: lasio.SectionItems,
    units: Dict[str, str] = UNITS,
    unit_buckets: Dict[str, str] = UNIT_BUCKETS,
    mnemonics: Dict[str, str] = MNEMONICS,
) -> List[ParsedLasSectionRow]:
    return [
        parse_section_row(
            row=row, units=units, unit_buckets=unit_buckets, mnemonics=mnemonics
        )
        for row in section
    ]


def map_log_data(
    log_data: List[List[Union[float, int]]],
    curve_mnemonics: List[str],
    mnemonics: Dict[str, str] = MNEMONICS,
) -> List[Dict[str, Union[float, int]]]:
    curve_mnemonics = [
        mnemonics.get(mnemonic.lower(), mnemonic) for mnemonic in curve_mnemonics
    ]

    result = [dict(zip(curve_mnemonics, log_row)) for log_row in log_data]

    return result


def validate_index_curve_mnemonic(
    las_file: lasio.LASFile,
    mnemonics: Dict[str, str] = MNEMONICS,
):
    """The index curve (i.e. first curve) must be depth.

    The index curve (i.e. first curve) must be depth. The only valid mnemonics for the
    depth are DEPT, DEPTH.
    """
    curves = las_file.curves

    if curves and mnemonics.get(curves[0].mnemonic.lower()) != 'md':
        raise ValueError('The index curve must be depth.')


def parse(file: str) -> ParsedLasFile:
    las_file = lasio.read(file, null_policy='none')

    validate_index_curve_mnemonic(las_file=las_file)

    # ~A (ASCII Log Data) section. Each column is a dataset.
    log_data: List[List[Union[float, int]]] = las_file.data.tolist()

    # ~C (Curve Information) section mnemonics. Each mnemonic is a dataset name.
    # Mnemonics are specified in the order they appear in the ~A (ASCII Log Data)
    # section.
    curve_mnemonics: List[str] = [curve.mnemonic for curve in las_file.curves]

    return ParsedLasFile(
        n_log_data_rows=len(log_data),
        well=parse_section(section=las_file.well),
        curves=parse_section(section=las_file.curves),
        params=parse_section(section=las_file.params),
        other=las_file.other,
        mapped_log_data=map_log_data(
            log_data=log_data, curve_mnemonics=curve_mnemonics
        ),
    )
