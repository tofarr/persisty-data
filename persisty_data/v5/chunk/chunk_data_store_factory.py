from typing import Optional, Tuple

from persisty.store_meta import get_meta, StoreMeta

from persisty_data.v5.chunk.model.chunk import create_stored_chunk_type
from persisty_data.v5.chunk.model.chunk_file_handle import (
    create_stored_chunk_file_handle_type,
)
from persisty_data.v5.chunk.model.chunk_upload import create_stored_chunk_upload_type
from persisty_data.v5.chunk.model.chunk_upload_part import (
    create_stored_chunk_upload_part_type,
)
from persisty_data.v5.data_store_factory_abc import DataStoreFactoryABC
from persisty_data.v5.data_store_security_abc import DataStoreSecurityABC


class ChunkDataStoreFactory(DataStoreFactoryABC):
    def create_all(
        self,
        name: str,
        datastore_security: DataStoreSecurityABC,
        permitted_content_types: Optional[Tuple[str, ...]] = None,
        max_size_in_bytes: int = 100 * 1024 * 1024,
        expire_in: Optional[int] = None,
    ) -> Optional[Tuple[StoreMeta, ...]]:
        chunk_store_meta = get_meta(create_stored_chunk_type(name))
        chunk_upload_store_meta = get_meta(
            create_stored_chunk_upload_type(
                name, permitted_content_types, datastore_security.get_upload_security()
            )
        )
        chunk_upload_part_store_meta = get_meta(
            create_stored_chunk_upload_part_type(
                name,
                datastore_security.get_upload_part_security(),
                chunk_upload_store_meta,
            )
        )
        chunk_file_handle_store_meta = get_meta(
            create_stored_chunk_file_handle_type(
                name, datastore_security.get_file_handle_security()
            )
        )
        return (
            chunk_store_meta,
            chunk_upload_part_store_meta,
            chunk_upload_store_meta,
            chunk_file_handle_store_meta,
        )
