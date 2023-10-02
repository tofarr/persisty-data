from datetime import datetime
from uuid import UUID

from persisty.attr.attr import Attr
from persisty.impl.dynamodb.partition_sort_index import PartitionSortIndex
from persisty.index.attr_index import AttrIndex
from persisty.index.unique_index import UniqueIndex
from persisty.security.store_access import NO_UPDATES
from persisty.security.store_security import INTERNAL_ONLY
from persisty.stored import stored
from schemey.schema import int_schema, str_schema

CHUNK_SIZE = 256 * 1024  # 256Kb


class Chunk:
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


def create_stored_chunk_type(store_name: str):
    store_type = store_name.title().replace("_", "")
    base_type = type(f"{store_type}Chunk", (Chunk,), {})
    result = stored(
        base_type,
        indexes=(
            AttrIndex("part_id"),
            PartitionSortIndex("part_id", "sort_key"),
            UniqueIndex(("part_id", "sort_key")),
            AttrIndex("upload_id"),
            PartitionSortIndex("upload_id", "sort_key"),
            UniqueIndex(("upload_id", "sort_key")),
        ),
        store_access=NO_UPDATES,
        store_security=INTERNAL_ONLY,
    )
    return result
