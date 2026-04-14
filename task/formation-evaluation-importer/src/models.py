import pathlib
from typing import Dict, List, Union

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, field_validator

from src.constants import MNEMONICS, UNIT_BUCKETS, UNITS


class CorvaModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


class EventProperties(CorvaModel):
    file_path: pathlib.Path = Field(..., alias='file_name')
    file_url: AnyHttpUrl

    @property
    def file_name(self) -> str:
        return self.file_path.name


class LasSectionRowData(CorvaModel):
    """Stores "~V", "~W", "~C" or "~P" las section row data.

    More info in "5.2 Line Delimiters"
      https://www.cwls.org/wp-content/uploads/2017/02/Las2_Update_Feb2017.pdf
    """

    mnemonic: str
    units: str = Field(..., alias='unit')
    value: str
    descr: str


class LasSectionRowMapping(CorvaModel):
    mnemonic: str
    unit: str
    bucket: str = Field(..., alias='unit')

    @field_validator('mnemonic')
    @classmethod
    def set_mnemonic(cls, v: str) -> str:
        return MNEMONICS.get(v.lower(), v)

    @field_validator('unit')
    @classmethod
    def set_unit(cls, v: str) -> str:
        return UNITS.get(v.lower(), v)

    @field_validator('bucket')
    @classmethod
    def set_bucket(cls, v: str) -> str:
        return UNIT_BUCKETS.get(v.lower(), 'Other')


class ParsedLasSectionRow(CorvaModel):
    data: LasSectionRowData
    mapping: LasSectionRowMapping


class ParsedLasFile(CorvaModel):
    """Stores parsed las file data.

    Attributes:
      n_log_data_rows: number of rows in ~A (ASCII Log Data) section.
      well: ~W (Well Information) section.
      curves: ~C (Curve Information) section.
      params: ~P (Parameter Information) section.
      other: ~O (Other Information) section.
      mapped_log_data: mapped log data.
    """

    n_log_data_rows: int
    well: List[ParsedLasSectionRow]
    curves: List[ParsedLasSectionRow]
    params: List[ParsedLasSectionRow]
    other: str
    mapped_log_data: List[Dict[str, Union[float, int]]]


class FormationEvaluationMetadataData(CorvaModel):
    params: List[ParsedLasSectionRow]
    well: List[ParsedLasSectionRow]
    curve: List[ParsedLasSectionRow]
    other: str


class FormationEvaluationMetadata(CorvaModel):
    asset_id: int
    timestamp: int
    company_id: int
    collection: str
    app: str
    provider: str
    data: FormationEvaluationMetadataData
    file: str = Field(..., alias='file_name')
    records_count: int
    version: int


class FormationEvaluationDataMetadata(CorvaModel):
    formation_evaluation_id: str
    file: str = Field(..., alias='file_name')


class FormationEvaluationData(CorvaModel):
    asset_id: int
    timestamp: int
    company_id: int
    collection: str
    app: str
    provider: str
    metadata: FormationEvaluationDataMetadata
    data: Dict[str, Union[float, int]]
    version: int


class SaveDataReponse(CorvaModel):
    inserted_ids: List[str]
