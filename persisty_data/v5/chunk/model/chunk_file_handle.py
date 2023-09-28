from uuid import UUID

from persisty.index.attr_index import AttrIndex
from persisty.security.store_access import NO_UPDATES
from persisty.security.store_security_abc import StoreSecurityABC
from persisty.stored import stored

from persisty_data.v5.file_handle import FileHandle


class ChunkFileHandle(FileHandle):
    stream_id: UUID


def create_stored_chunk_file_handle_type(
    store_name: str, store_security: StoreSecurityABC
):
    name = store_name.title().replace("_", "")
    chunk_type = type(name, (ChunkFileHandle,), {})
    stored_chunk = stored(
        chunk_type,
        indexes=(AttrIndex("stream_id"),),
        store_access=NO_UPDATES,
        store_security=store_security,
    )
    return stored_chunk
