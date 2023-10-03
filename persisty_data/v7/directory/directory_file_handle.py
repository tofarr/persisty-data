from datetime import datetime
from typing import Optional

from persisty.attr.attr import Attr
from persisty.attr.attr_type import AttrType
from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.stored import stored

from persisty_data.v7.generator.content_type_generator import ContentTypeGenerator


@stored(
    key_config=AttrKeyConfig('file_name', AttrType.STR)
)
class DirectoryFileHandle:
    """Metadata about a file"""
    id: str  # Combo of store_name and file_name
    store_name: str
    file_name: str
    content_type: Optional[str] = Attr(create_generator=ContentTypeGenerator())
    etag: str
    size_in_bytes: int
    created_at: datetime
    updated_at: datetime
