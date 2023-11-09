from datetime import datetime
from uuid import UUID

from persisty.attr.attr import Attr
from persisty.impl.dynamodb.partition_sort_index import PartitionSortIndex
from persisty.index.unique_index import UniqueIndex
from persisty.security.store_security import INTERNAL_ONLY
from persisty.stored import stored

from persisty_data.generator.part_number_generator import PartNumberGenerator


@stored(
    indexes=(
        UniqueIndex(("upload_id", "part_number")),
        PartitionSortIndex("upload_id", "part_number"),
    ),
    store_security=INTERNAL_ONLY,
)
class PersistyUploadPart:
    id: UUID
    upload_id: str
    part_number: int = Attr(create_generator=PartNumberGenerator("upload_id"))
    created_at: datetime
    updated_at: datetime
