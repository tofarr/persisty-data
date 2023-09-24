from datetime import datetime
from typing import Optional

from persisty.attr.attr import Attr
from persisty.stored import stored

from persisty_data.v4.file_handle_status import FileHandleStatus


class FileHandle:
    """
    File handle - not directly creatable or updatable
    """

    key: str
    content_type: Optional[str] = None  # Can limit allowed content types using schema
    size_in_bytes: int = Attr(
        creatable=False, updatable=False
    )  # can limit size using schema
    etag: str = Attr(creatable=False, updatable=False)
    download_url: str = Attr(creatable=False, updatable=False)
    created_at: datetime
    updated_at: datetime
    expire_at: Optional[datetime] = None


StoredFileHandle = stored(FileHandle)
