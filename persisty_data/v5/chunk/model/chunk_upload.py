from uuid import UUID

from persisty.index.attr_index import AttrIndex
from persisty.security.store_access import NO_UPDATES
from persisty.security.store_security import INTERNAL_ONLY
from persisty.stored import stored

from persisty_data.v5.upload import Upload


class ChunkUpload(Upload):
    stream_id: UUID


def create_stored_chunk_upload_type(store_name: str):
    name = (store_name.title().replace("_", ""),)
    chunk_type = type(f"{name}ChunkUpload", (ChunkUpload,), {})
    stored_chunk = stored(
        chunk_type, indexes=(AttrIndex("item_key"),), store_access=NO_UPDATES, store_security=INTERNAL_ONLY
    )
    return stored_chunk
