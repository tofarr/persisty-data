from datetime import datetime
from typing import Optional
from uuid import UUID

from persisty.attr.attr import Attr
from persisty.security.store_access import NO_UPDATES
from persisty.security.store_security import StoreSecurity
from persisty.stored import stored

from persisty_data.v5.upload_part import UploadPart


class Upload:
    """
    Creatable but not updatable
    """
    id: UUID
    item_key: str
    content_type: Optional[str]
    max_part_size_in_bytes: int = Attr(creatable=False)
    max_number_of_parts: int = Attr(creatable=False)
    expire_at: datetime = Attr(creatable=False)
    created_at: datetime


StoredUpload: Upload = stored(Upload, store_access=NO_UPDATES)

UploadPartStored = stored(
    UploadPart,
    store_access=NO_UPDATES,
    store_security=StoreSecurity(default_access=NO_UPDATES, api_access=NO_UPDATES)
    #TODO: need factory here...
)
