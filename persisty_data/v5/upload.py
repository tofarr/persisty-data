from datetime import datetime
from typing import Optional
from uuid import UUID


class Upload:
    """
    Creatable but not updatable
    """
    id: UUID
    item_key: str
    content_type: Optional[str]
    max_part_size_in_bytes: int
    expire_at: datetime
    created_at: datetime
    updated_at: datetime
