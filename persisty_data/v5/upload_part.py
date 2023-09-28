from datetime import datetime
from uuid import UUID

from persisty.attr.attr import Attr
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.security.store_access import StoreAccess, NO_UPDATES
from persisty.stored import stored

from persisty_data.v5.upload_part_number_generator import UploadPartNumberGenerator


class UploadPart:
    """
    Creatable but not updatable or deletable
    """

    id: UUID
    upload_id: UUID
    part_number: int
    upload_url: str
    max_size: int
    created_at: datetime


UPLOAD_PART_ACCESS = StoreAccess(update_filter=EXCLUDE_ALL, delete_filter=EXCLUDE_ALL)
UploadPartStored = stored(UploadPart, store_access=NO_UPDATES)
