from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from persisty.attr.attr import Attr
from persisty.result_set import result_set_dataclass_for
from persisty.search_filter.search_filter_factory import search_filter_dataclass_for
from persisty.search_order.search_order_factory import search_order_dataclass_for
from persisty.store_meta import get_meta
from persisty.stored import stored


class FileHandle:
    """Object containing metadata about a file"""

    file_name: str
    content_type: Optional[str]
    etag: str = Attr()
    size_in_bytes: int
    download_url: str = Attr(permitted_filter_ops=tuple(), sortable=False)
    updated_at: datetime


StoredFileHandle = stored(FileHandle)
FileHandleSearchFilter = search_filter_dataclass_for(get_meta(StoredFileHandle))
FileHandleSearchOrder = search_order_dataclass_for(get_meta(StoredFileHandle))
FileHandleResultSet = result_set_dataclass_for(StoredFileHandle)
