from typing import Dict, List, Optional, Union

import pydantic


class Record(pydantic.BaseModel):
    measured_depth: float
    data: dict


class InSummary(pydantic.BaseModel):
    id: str = pydantic.Field(..., alias='_id')
    measured_depth: float


class UpdateSummary(pydantic.BaseModel):
    data: Dict[str, Union[float, List[Optional[float]]]]


class Summary(pydantic.BaseModel):
    asset_id: int
    version: int
    measured_depth: float
    log_identifier: str
    data: Dict[str, Union[float, List[Optional[float]]]]
