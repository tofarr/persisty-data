from dataclasses import dataclass
from typing import List, Optional, Type

from persisty.security.store_security_abc import StoreSecurityABC
from persisty.store_meta import StoreMeta

from persisty_data.v6.data_store_abc import DataStoreABC
from persisty_data.v6.filesystem.file_system_file_handle import FileSystemFileHandle, DATA_DIRECTORY
from persisty_data.v6.filesystem.file_system_upload import FileSystemUpload
from persisty_data.v6.filesystem.file_system_upload_part import FileSystemUploadPart
from persisty_data.v6.model.file_handle import create_stored_file_handle_type, FileHandle
from persisty_data.v6.model.upload import create_stored_upload_type, Upload
from persisty_data.v6.model.upload_part import create_stored_upload_part_type, UploadPart


@dataclass
class FileSystemDataStore(DataStoreABC):
    data_directory: str = DATA_DIRECTORY

    def create_store_meta(self, name: str, store_security: Optional[StoreSecurityABC] = None) -> List[StoreMeta]:

        data_directory = self.data_directory

        type_name = name.title().replace("_", "")
        file_handle = type(type_name, (FileSystemFileHandle, DataDirectoryMixin), {})
        upload = type(type_name+"Upload", (FileSystemUpload, DataDirectoryMixin), {})
        upload_part = type(type_name+"UploadPart", (FileSystemUploadPart, DataDirectoryMixin), {})

        # noinspection PyTypeChecker
        return [
            create_stored_file_handle_type(name, file_handle, store_security),
            create_stored_upload_type(name, upload),
            create_stored_upload_part_type(name, upload_part),
        ]
