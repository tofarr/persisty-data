from datetime import datetime
from typing import Optional

from persisty_data.v4.file_handle_status import FileHandleStatus


class ChunkedFileHandle:
    """Object representing a file in a store"""

    key: str
    status: FileHandleStatus
    stream_id: Optional[str] = None
    content_type: Optional[str] = None
    size_in_bytes: Optional[int] = None
    etag: Optional[str] = None
    created_at: datetime
    updated_at: datetime
