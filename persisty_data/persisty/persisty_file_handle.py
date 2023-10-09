from datetime import datetime
from typing import Optional
from uuid import UUID

from persisty.attr.attr import Attr
from persisty.impl.dynamodb.partition_sort_index import PartitionSortIndex
from persisty.index.unique_index import UniqueIndex
from persisty.security.store_security import INTERNAL_ONLY
from persisty.stored import stored

from persisty_data.generator.content_type_generator import ContentTypeGenerator
from persisty_data.generator.file_handle_id_generator import FileHandleIdGenerator


@stored(
    indexes=(
        UniqueIndex(("store_name", "file_name")),
        PartitionSortIndex("store_name", "file_name"),
    ),
    store_security=INTERNAL_ONLY,
)
class PersistyFileHandle:
    """Metadata about a file"""

    id: str = Attr(create_generator=FileHandleIdGenerator())  # Combo of store_name and file_name
    store_name: str
    file_name: str
    upload_id: Optional[UUID] = None
    content_type: Optional[str] = Attr(create_generator=ContentTypeGenerator())
    etag: str
    size_in_bytes: int
    created_at: datetime
    updated_at: datetime
