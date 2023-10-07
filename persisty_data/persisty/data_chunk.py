from datetime import datetime
from uuid import UUID

from persisty.attr.attr import Attr
from persisty.impl.dynamodb.partition_sort_index import PartitionSortIndex
from persisty.index.unique_index import UniqueIndex
from persisty.security.store_security import INTERNAL_ONLY
from persisty.stored import stored
from schemey.schema import int_schema


@stored(
    indexes=(
        UniqueIndex(("upload_id", "sort_key")),
        PartitionSortIndex("upload_id", "sort_key"),
    ),
    store_security=INTERNAL_ONLY,
)
class DataChunk:
    id: UUID
    upload_id: str = Attr(updatable=False)
    part_number: int = Attr(updatable=False, schema=int_schema(minimum=0))
    chunk_number: int = Attr(updatable=False, schema=int_schema(minimum=0))
    sort_key: int = Attr(updatable=False, schema=int_schema(minimum=0))
    data: bytes
    created_at: datetime
    updated_at: datetime


def get_sort_key(part_number: int, chunk_number: int):
    return part_number * 1024 * 1024 * 64 + chunk_number
