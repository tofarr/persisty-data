from typing import Optional

from persisty.stored import stored


class Downloadable:
    store_name: str
    key: str
    content_type: Optional[str]
    etag: str
    size: int
    url: str
