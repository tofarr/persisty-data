from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from persisty.result_set import ResultSet


@dataclass
class UploadHandle:
    id: str
    data_id: str
    group_id: Optional[str]
    content_type: Optional[str]
    subject_id: Optional[str]
    parts: ResultSet[str]
    part_count: int
    created_at: datetime
    updated_at: datetime
    expire_at: datetime
