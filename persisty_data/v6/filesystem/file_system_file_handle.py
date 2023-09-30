import os
from pathlib import Path
from typing import BinaryIO

from persisty.store_meta import get_meta

from persisty_data.v6.model.file_handle import FileHandle
from persisty_data.v6.model.store_model_abc import StoreModelABC

DATA_DIRECTORY = "persisty_data"


class FileSystemFileHandle(FileHandle, StoreModelABC):

    def get_reader(self) -> BinaryIO:
        store_meta = self.get_file_handle_store_meta()
        path = key_to_path(Path(self.get_data_directory(), store_meta.name), self.key)
        return open(path, "rb")

    @classmethod
    def base_store_name(cls):
        result = get_meta(cls).name
        return result

    @staticmethod
    def get_data_directory() -> str:
        return DATA_DIRECTORY


def key_to_path(base_directory: Path, key: str):
    path = Path(base_directory, key)
    assert os.path.normpath(path) == str(path)  # Prevent ../ shenanigans
    return path
