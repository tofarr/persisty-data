from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class FileHandle:
    """Object containing metadata about a file"""

    file_name: str
    content_type: Optional[str]
    etag: str
    size_in_bytes: int
    download_url: str
    updated_at: datetime
