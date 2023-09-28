from datetime import datetime
from typing import Optional
from uuid import UUID

from persisty.security.store_access import READ_ONLY
from persisty.stored import stored


class ChunkFileHandle:
    """
    File handle - not directly creatable or updatable
    """

    key: str
    upload_id: UUID
    content_type: Optional[str] = None  # Can limit allowed content types using schema
    size_in_bytes: int  # can limit size using schema
    etag: str
    download_url: str  # may be signed or not depending on implementation
    created_at: datetime
    updated_at: datetime
    # expire_at: Optional[datetime] = None


StoredFileHandle = stored(ChunkFileHandle, store_security=READ_ONLY)
