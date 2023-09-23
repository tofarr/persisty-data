from typing import Optional

from persisty.stored import stored


# @stored
class DataItem:
    key: str
    stream_id: str
    content_type: Optional[str]
    etag: str
    size: int
