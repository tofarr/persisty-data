import hashlib
from _typeshed import ReadableBuffer
from dataclasses import dataclass, field
from io import IOBase
from typing import BinaryIO, Optional

from persisty.store.store_abc import StoreABC
from persisty.store_meta import get_meta

from persisty_data.v7.directory.directory_file_handle import DirectoryFileHandle


@dataclass
class DirectoryFileHandleWriter(IOBase):
    writer: BinaryIO
    file_name: str
    content_type: Optional[str]
    size_in_bytes: int = 0
    hash: hashlib.md5 = field(default_factory=hashlib.md5)
    file_handle_store: StoreABC[DirectoryFileHandle] = field(
        default_factory=get_meta(DirectoryFileHandle).create_store
    )

    def __enter__(self):
        return self.writer.__enter__()

    def write(self, __b: ReadableBuffer) -> int | None:
        self.hash.update(__b)
        self.size_in_bytes += len(__b)
        result = self.writer.write(__b)
        return result

    def __exit__(self, exc_type, exc_val, exc_tb):
        result = self.writer.__exit__(exc_type, exc_val, exc_tb)
        self.file_handle_store.create(DirectoryFileHandle(
            file_name=self.file_name,
            content_type=self.content_type,
            etag=self.hash.hexdigest(),
            size_in_bytes=self.size_in_bytes,
        ))
        return result
