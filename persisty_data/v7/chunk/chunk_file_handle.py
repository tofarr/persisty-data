from datetime import datetime
from typing import Optional

from persisty.attr.attr import Attr
from schemey.schema import int_schema

from persisty_data.v7.generator.content_type_generator import ContentTypeGenerator
from persisty_data.v7.generator.pattern_generator import PatternGenerator


class ChunkFileHandle:
    """
    File handle - not directly creatable or updatable
    """

    key: str
    upload_id: str
    content_type: Optional[str] = Attr(create_generator=ContentTypeGenerator())
    size_in_bytes: int = Attr(schema=int_schema(maximum=256 * 1024 * 1024))
    etag: str
    download_url: str = Attr(
        creatable=False,
        create_generator=PatternGenerator("key", "/data/{store_name}/{value}"),
    )
    subject_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    expire_at: Optional[datetime] = None
