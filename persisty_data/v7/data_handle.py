from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import UUID

from persisty_data.v7.data_item_status import DataItemStatus


@dataclass
class DataHandle:
    id: str
    group_id: Optional[str]
    content_type: Optional[str]
    etag: str
    size_in_bytes: int
    download_url: str
    subject_id: Optional[str]
    created_at: datetime
    updated_at: datetime
