from datetime import datetime

from persisty.attr.attr import Attr
from persisty.attr.generator.default_value_generator import DefaultValueGenerator

from persisty_data.v6.generator.pattern_generator import PatternGenerator
from persisty_data.v7.generator.part_number_generator import PartNumberGenerator


@stored
class ChunkUploadPart:
    id: str
    subject_id: str
    upload_id: str
    part_number: int = Attr(create_generator=PartNumberGenerator("upload_id"))
    size_in_bytes: int = Attr(
        creatable=False, create_generator=DefaultValueGenerator(0)
    )
    upload_url: str = Attr(
        creatable=False,
        create_generator=PatternGenerator(
            "id", "/data/{store_name}-upload-part/{value}"
        ),
    )
    created_at: datetime
    updated_at: datetime
