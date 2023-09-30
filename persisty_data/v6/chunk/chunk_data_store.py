from typing import List, Optional

from persisty.security.store_security_abc import StoreSecurityABC
from persisty.store_meta import StoreMeta

from persisty_data.v6.chunk.chunk import create_stored_chunk_type
from persisty_data.v6.chunk.chunk_file_handle import ChunkFileHandle
from persisty_data.v6.chunk.chunk_upload import ChunkUpload
from persisty_data.v6.chunk.chunk_upload_part import ChunkUploadPart
from persisty_data.v6.data_store_abc import DataStoreABC
from persisty_data.v6.model.file_handle import create_stored_file_handle_type
from persisty_data.v6.model.upload import create_stored_upload_type
from persisty_data.v6.model.upload_part import create_stored_upload_part_type


class ChunkDataStore(DataStoreABC):

    def create_store_meta(self, name: str, store_security: Optional[StoreSecurityABC] = None) -> List[StoreMeta]:
        return [
            create_stored_file_handle_type(name, ChunkFileHandle, store_security),
            create_stored_upload_type(name, ChunkUpload),
            create_stored_upload_part_type(name, ChunkUploadPart),
            create_stored_chunk_type(name)
        ]
