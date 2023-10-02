from datetime import datetime
from uuid import UUID

from persisty.attr.attr import Attr
from persisty.impl.dynamodb.partition_sort_index import PartitionSortIndex
from persisty.index.unique_index import UniqueIndex
from persisty.stored import stored
from schemey.schema import int_schema


@stored(
    indexes=(
        UniqueIndex(("upload_id", "sort_key")),
        PartitionSortIndex("upload_id", "sort_key"),
    )
)
class DataChunk:
    id: UUID
    upload_id: str = Attr(updatable=False)
    part_id: str = Attr(updatable=False)
    part_number: int = Attr(updatable=False, schema=int_schema(minimum=0))
    chunk_number: int = Attr(updatable=False, schema=int_schema(minimum=0))
    # sort key is part_number * 1024 * 1024 + chunk_number - so I guess we can have a max
    # file size of 256Gb - this seems like overkill for this sort of store - you would definitely want to use
    # s3 or directory long before getting to this point.
    sort_key: int = Attr(updatable=False, schema=int_schema(minimum=0))
    data: bytes
    created_at: datetime
    updated_at: datetime
