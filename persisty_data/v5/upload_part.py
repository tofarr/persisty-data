from datetime import datetime
from uuid import UUID

from persisty.attr.attr import Attr
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.security.store_access import StoreAccess
from persisty.security.store_security import StoreSecurity
from persisty.stored import stored


class UploadPart:
    """
    Creatable but not updatable or deletable
    """
    id: UUID
    upload_id: UUID
    part_number: int = Attr(creatable=False)
    upload_url: str = Attr(creatable=False)
    max_size: int = Attr(creatable=False)
    created_at: datetime


UPLOAD_PART_ACCESS = StoreAccess(update_filter=EXCLUDE_ALL, delete_filter=EXCLUDE_ALL)
UploadPartStored = stored(
    UploadPart,
    store_access=UPLOAD_PART_ACCESS,
    store_security=StoreSecurity(default_access=UPLOAD_PART_ACCESS, api_access=UPLOAD_PART_ACCESS)
    #TODO: need factory here...
)
