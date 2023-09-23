from datetime import datetime
from typing import Optional

from persisty.stored import stored


@stored
class Stream:
    id: str
    content_type: Optional[str]
    complete: bool
    created_at: datetime
    updated_at: datetime
