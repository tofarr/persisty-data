from datetime import datetime
from uuid import UUID


class UploadPart:
    """
    Creatable but not updatable or deletable
    """
    id: UUID
    upload_id: UUID
    part_number: int
    upload_url: str
    created_at: datetime
    updated_at: datetime