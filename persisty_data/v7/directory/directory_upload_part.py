from datetime import datetime
from uuid import UUID

from persisty.attr.attr import Attr
from persisty.attr.generator.default_value_generator import DefaultValueGenerator
from persisty.impl.dynamodb.partition_sort_index import PartitionSortIndex
from persisty.index.unique_index import UniqueIndex
from persisty.stored import stored

from persisty_data.v6.generator.pattern_generator import PatternGenerator
from persisty_data.v7.generator.part_number_generator import PartNumberGenerator


@stored(
    indexes=(
        UniqueIndex(("upload_id", "part_number")),
        PartitionSortIndex("upload_id", "part_number"),
    )
)
class DirectoryUploadPart:
    id: UUID
    upload_id: UUID
    part_number: int = Attr(create_generator=PartNumberGenerator("upload_id"))
    created_at: datetime
    updated_at: datetime
