from datetime import datetime
from typing import Optional

from persisty.attr.attr import Attr

from persisty_data.v4.file_handle_status import FileHandleStatus


class FileHandle:
    """
    File handle - not directly creatable or updatable
    """
    key: str
    status: FileHandleStatus = Attr(creatable=False, updatable=False)
    content_type: Optional[str] = None  # Can limit allowed content types using schema
    size_in_bytes: Optional[int] = Attr(creatable=False, updatable=False)  # can limit size using schema
    etag: Optional[str] = Attr(creatable=False, updatable=False)
    download_url: Optional[str] = Attr(creatable=False, updatable=False)
    created_at: datetime
    updated_at: datetime
    expire_at: Optional[datetime] = None