from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.request import urlopen

from persisty.attr.attr import Attr
from persisty.errors import PersistyError

from persisty_data.v4.file_handle_status import FileHandleStatus


class FileHandle:
    """
    Object representing a file in a store. When output, the handle object is always a string
    When input, the handle object can be a string, FileUpload or Path. Only strings are accepted
    as input from REST / Graphql - the action_factory for the store typically includes an action for
    creating an upload form which may yield a FormUpload object.
    """

    key: str
    status: FileHandleStatus = Attr(creatable=False, updatable=False)
    content_type: Optional[str] = None
    handle: Optional[
        str
    ] = None  # Can be a url, local resource, or even base64 encoded data
    size_in_bytes: Optional[int] = Attr(creatable=False, updatable=False)
    etag: Optional[str] = Attr(creatable=False, updatable=False)
    created_at: datetime
    updated_at: datetime

    def open_for_read(self):
        if self.status != FileHandleStatus.READY or not self.handle:
            raise PersistyError("not_ready")
        handle = self.handle
        if isinstance(handle, str):
            if not handle.startswith("http://") and not handle.startswith("https://"):
                raise PersistyError("invalid_handle")
            return urlopen(handle)
        if isinstance(handle, Path):
            return open(handle, "rb")
        from starlette.datastructures import UploadFile

        if isinstance(handle, UploadFile):
            return handle
