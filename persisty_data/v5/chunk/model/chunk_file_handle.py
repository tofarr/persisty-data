from datetime import datetime
from typing import Optional
from uuid import UUID

from persisty.index.attr_index import AttrIndex
from persisty.security.store_security import INTERNAL_ONLY
from persisty.stored import stored

from persisty_data.v5.file_handle import StoredFileHandle, FileHandle


class ChunkFileHandle:
    key: str
    stream_id: UUID
    content_type: Optional[str]
    size_in_bytes: int
    etag: str
    created_at: datetime
    updated_at: datetime
    expire_at: Optional[datetime] = None

    def to_file_handle(self, download_url_pattern: str) -> FileHandle:
        return StoredFileHandle(
            key=self.key,
            content_type=self.content_type,
            size_in_bytes=self.size_in_bytes,
            etag=self.etag,
            download_url=download_url_pattern.format(key=self.key),
            created_at=self.created_at,
            updated_at=self.updated_at,
            expire_at=self.expire_at
        )


def create_stored_chunk_file_handle_type(store_name: str):
    name = (store_name.title().replace("_", ""),)
    chunk_type = type(f"{name}ChunkFileHandle", (ChunkFileHandle,), {})
    stored_chunk = stored(
        chunk_type, indexes=(AttrIndex("stream_id"),), store_security=INTERNAL_ONLY
    )
    return stored_chunk
