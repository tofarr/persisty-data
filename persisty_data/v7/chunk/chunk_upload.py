from datetime import datetime
from typing import Optional

from persisty.attr.attr import Attr

from persisty_data.v7.generator.content_type_generator import ContentTypeGenerator
from persisty_data.v7.generator.future_timestamp_generator import (
    FutureTimestampGenerator,
)


class ChunkUpload:
    """
    Creatable but not updatable
    """

    id: str
    item_key: str
    content_type: Optional[str] = Attr(
        create_generator=ContentTypeGenerator("item_key")
    )
    max_part_size_in_bytes: int = 256 * 1024  # 256kb
    max_number_of_parts: int = 1024  # Default max size is 256Mb
    subject_id: Optional[str] = None
    expire_at: datetime = Attr(create_generator=FutureTimestampGenerator(3600))
    created_at: datetime
