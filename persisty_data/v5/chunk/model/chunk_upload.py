from enum import Enum
from typing import Optional
from uuid import UUID

from persisty.attr.attr import Attr, DEFAULT_PERMITTED_FILTER_OPS
from persisty.attr.attr_type import AttrType
from persisty.attr.generator.uuid_generator import UuidGenerator
from persisty.index.attr_index import AttrIndex
from persisty.security.store_access import NO_UPDATES
from persisty.security.store_security_abc import StoreSecurityABC
from persisty.stored import stored
from schemey import schema_from_type

from persisty_data.v5.upload import Upload


class ChunkUpload(Upload):
    stream_id: UUID = Attr(creatable=False, create_generator=UuidGenerator())


def create_stored_chunk_upload_type(
    store_name: str,
    permitted_content_types: Optional[str],
    store_security: StoreSecurityABC,
):
    name = (store_name.title().replace("_", ""),)
    content_type_schema = schema_from_type(
        Enum(f"{name}ContentType", [(c, c) for c in permitted_content_types])
    )
    content_type_attr = Attr(
        name="content_type",
        attr_type=AttrType.STR,
        schema=content_type_schema,
        updatable=False,
        permitted_filter_ops=DEFAULT_PERMITTED_FILTER_OPS,
    )
    chunk_type = type(
        f"{name}Upload", (ChunkUpload,), {"content_type": content_type_attr}
    )
    stored_chunk = stored(
        chunk_type,
        indexes=(AttrIndex("item_key"),),
        store_access=NO_UPDATES,
        store_security=store_security,
    )
    return stored_chunk
