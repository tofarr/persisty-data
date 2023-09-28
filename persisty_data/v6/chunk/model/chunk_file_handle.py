from pathlib import Path
from typing import BinaryIO
from urllib.request import urlopen

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.errors import PersistyError
from persisty.finder.stored_finder_abc import find_stored_by_name
from persisty.store_meta import get_meta

from persisty_data.v6.model.file_handle import FileHandle


class ChunkFileHandle(FileHandle):
    """
    File handle - not directly creatable or updatable
    """

    upload_id: str

    def get_reader(self) -> BinaryIO:
        """
        Get a reader for this file (May read locally or remote - default implementation tries to read download url
        """
        download_url = self.download_url
        if isinstance(download_url, str):
            # noinspection HttpUrlsUsage
            if not download_url.startswith("http://") and not download_url.startswith(
                "https://"
            ):
                raise PersistyError("invalid_download_url")
            return urlopen(download_url)
        if isinstance(download_url, Path):
            return open(download_url, "rb")

    def after_delete(self):
        self.get_chunk_store_meta().delete_all(AttrFilter('upload_id', AttrFilterOp.eq, self.upload_id))

    @classmethod
    def get_chunk_store_meta(cls):
        chunk_store_meta = getattr(cls, "__chunk_store_meta__", None)
        if not chunk_store_meta:
            chunk_store_meta = find_stored_by_name(cls.base_store_name() + "_chunk")
        return chunk_store_meta

    @classmethod
    def base_store_name(cls):
        result = get_meta(cls).name
        return result