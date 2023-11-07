import hashlib
from dataclasses import dataclass, field
from types import TracebackType
from typing import Optional

from persisty.finder.store_meta_finder_abc import find_store_meta_by_name
from persisty.store.store_abc import StoreABC

from persisty_data.persisty_store.data_chunk_writer import DataChunkWriter
from persisty_data.persisty_store.persisty_file_handle import PersistyFileHandle


@dataclass(kw_only=True)
class PersistyFileHandleWriter(DataChunkWriter):
    store_name: str
    file_name: str
    content_type: Optional[str] = None
    file_handle_store: StoreABC[PersistyFileHandle] = field(
        default_factory=lambda: find_store_meta_by_name(
            "persisty_file_handle"
        ).create_store()
    )
    size_in_bytes: int = 0
    hash: hashlib.md5 = field(default_factory=hashlib.md5)

    def _create_chunk(self):
        print(f"Creating Chunk...{self}")
        self.size_in_bytes += len(self.buffer)
        self.hash.update(self.buffer)
        super()._create_chunk()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        super().__exit__(exc_type, exc_val, exc_tb)
        key = f"{self.store_name}/{self.file_name}"
        file_handle = self.file_handle_store.read(key)
        updates = PersistyFileHandle(
            id=key,
            store_name=self.store_name,
            file_name=self.file_name,
            upload_id=self.upload_part.upload_id,
            content_type=self.content_type,
            etag=self.hash.hexdigest(),
            size_in_bytes=self.size_in_bytes,
        )
        if file_handle:
            # noinspection PyProtectedMember
            self.file_handle_store._update(key, file_handle, updates)
        else:
            self.file_handle_store.create(updates)
