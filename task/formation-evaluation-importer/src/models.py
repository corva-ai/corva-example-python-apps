import pathlib
from typing import Dict, List, Union

import pydantic


class EventProperties(pydantic.BaseModel):
    file_path: pathlib.Path = pydantic.Field(..., alias='file_name')
    file_url: pydantic.AnyHttpUrl

    @property
    def file_name(self) -> str:
        return self.file_path.name


class LasSectionRowData(pydantic.BaseModel):
    """Stores "~V", "~W", "~C" or "~P" las section row data.

    More info in "5.2 Line Delimiters"
      https://www.cwls.org/wp-content/uploads/2017/02/Las2_Update_Feb2017.pdf
    """

    mnemonic: str
    units: str = pydantic.Field(..., alias='unit')
    value: str
    descr: str


class LasSectionRowMapping(pydantic.BaseModel):
    mnemonic: str
    unit: str
    bucket: str


class ParsedLasSectionRow(pydantic.BaseModel):
    data: LasSectionRowData
    mapping: LasSectionRowMapping


class ParsedLasFile(pydantic.BaseModel):
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


class FormationEvaluationMetadataData(pydantic.BaseModel):
    params: List[ParsedLasSectionRow]
    well: List[ParsedLasSectionRow]
    curve: List[ParsedLasSectionRow]
    other: str


class FormationEvaluationMetadata(pydantic.BaseModel):
    asset_id: int
    timestamp: int
    company_id: int
    collection: str
    app: str
    provider: str
    data: FormationEvaluationMetadataData
    file: str = pydantic.Field(..., alias='file_name')
    records_count: int
    version: int


class FormationEvaluationDataMetadata(pydantic.BaseModel):
    formation_evaluation_id: str
    file: str = pydantic.Field(..., alias='file_name')


class FormationEvaluationData(pydantic.BaseModel):
    asset_id: int
    timestamp: int
    company_id: int
    collection: str
    app: str
    provider: str
    metadata: FormationEvaluationDataMetadata
    data: Dict[str, Union[float, int]]
    version: int


class SaveDataReponse(pydantic.BaseModel):
    inserted_ids: List[str]
