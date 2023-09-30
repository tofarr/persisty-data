from pathlib import Path
from typing import AsyncIterator

from persisty.store_meta import get_meta

from persisty_data.v6.filesystem.file_system_file_handle import DATA_DIRECTORY
from persisty_data.v6.model.upload_part import UploadPart


class FileSystemUploadPart(UploadPart):

    async def save_content(self, content: AsyncIterator[bytes]):
        upload_directory = Path(self.get_data_directory(), get_meta(self.__class__).name)
        upload_directory.mkdir(parents=True, exist_ok=True)
        path = Path(upload_directory, str(self.id))
        with open(path, "wb") as writer:
            size_in_bytes = 0
            async for data in content:
                writer.write(data)
                size_in_bytes += len(data)
            return size_in_bytes

    @staticmethod
    def get_data_directory() -> str:
        return DATA_DIRECTORY
