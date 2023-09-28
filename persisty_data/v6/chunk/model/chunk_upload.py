from datetime import datetime
from typing import Optional
from uuid import UUID

from persisty.security.store_access import NO_UPDATES
from persisty.security.store_security import StoreSecurity
from persisty.stored import stored

from persisty_data.v6.chunk.model.chunk_upload_part import ChunkUploadPart


class ChunkUpload:
    """
    Creatable but not updatable
    """
    id: UUID
    item_key: str
    content_type: Optional[str]
    max_part_size_in_bytes: int
    max_number_of_parts: int
    expire_at: datetime
    created_at: datetime


StoredUpload: ChunkUpload = stored(ChunkUpload, store_access=NO_UPDATES)

ChunkUploadPartStored = stored(
    ChunkUploadPart,
    store_access=NO_UPDATES,
    store_security=StoreSecurity(default_access=NO_UPDATES, api_access=NO_UPDATES)
    # TODO: need factory here...
)
