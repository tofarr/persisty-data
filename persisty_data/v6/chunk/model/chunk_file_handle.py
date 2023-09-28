from pathlib import Path
from typing import BinaryIO
from urllib.request import urlopen

from persisty.errors import PersistyError

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
