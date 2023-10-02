from datetime import datetime
from typing import Optional
from uuid import UUID

from persisty.attr.attr import Attr
from persisty.impl.dynamodb.partition_sort_index import PartitionSortIndex
from persisty.index.unique_index import UniqueIndex
from persisty.stored import stored

from persisty_data.v7.generator.content_type_generator import ContentTypeGenerator


@stored(
    indexes=(
        UniqueIndex(("store_name", "file_name")),
        PartitionSortIndex("store_name", "file_name"),
    )
)
class PersistyUploadHandle:
    """
    Metadata about an upload
    """
    id: UUID
    store_name: str
    file_name: str
    content_type: Optional[str] = Attr(
        create_generator=ContentTypeGenerator("file_name")
    )
    expire_at: datetime
    created_at: datetime
    updated_at: datetime
