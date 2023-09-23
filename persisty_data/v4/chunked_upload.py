from dataclasses import dataclass
from datetime import datetime
from typing import List


@dataclass
class ChunkedUpload:
    id: str
    store_name: str
    key: str
    presigned_urls: List[str]
    expire_at: datetime
