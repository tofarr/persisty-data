import dataclasses
from datetime import datetime
from typing import Optional
from uuid import UUID

from persisty.attr.attr import Attr
from persisty.impl.dynamodb.partition_sort_index import PartitionSortIndex
from persisty.index.attr_index import AttrIndex
from persisty.index.unique_index import UniqueIndex
from persisty.security.store_access import NO_UPDATES, NO_ACCESS
from persisty.security.store_security_abc import StoreSecurityABC
from persisty.store_meta import get_meta, StoreMeta
from persisty.stored import stored

from persisty_data.v5.chunk.chunk_upload_part_stream_id_generator import (
    ChunkUploadPartStreamIdGenerator,
)
from persisty_data.v5.upload_part import UploadPart, UploadPartStored
from persisty_data.v5.upload_part_number_generator import UploadPartNumberGenerator


class ChunkUploadPart:
    id: UUID
    stream_id: UUID
    upload_id: UUID
    part_number: int = Attr(create_generator=UploadPartNumberGenerator())
    upload_url: str
    max_size: int
    created_at: datetime


# Maybe the secured store should produce a meta override store... with the meta override store copying on input and output
# Do we have 2? an internal and an external? That feels wrong...
# Do we make stream_id, part_number, upload_url, max_size always generated?
# This kind of hampers the action which creates uploads with parts - it makes generating parts slow.
# Feels like internal mode and external mode need separation
# Feels like we need a way to remove unwanted attrs from external API - maybe get rid of "get_api_access" and replace with "get_api_meta"
# or we could just build a standard interface...

"""
ChunkUploadPartStored = stored(ChunkUploadPart)


def chunk_upload_part(upload_part: UploadPart, stream_id: UUID) -> ChunkUploadPart:
    result = ChunkUploadPartStored(
        **{a.name: getattr(upload_part, a.name) for a in get_meta(UploadPart).attrs}
    )
    result.stream_id = stream_id
    return result
"""


def to_upload_part(chunk_upload_part: ChunkUploadPart) -> UploadPart:
    result = UploadPartStored(
        **{
            k: v
            for k, v in dataclasses.asdict(chunk_upload_part).items()
            if k != "stream_id"
        }
    )
    return result


def to_chunk_upload_part(upload_part: UploadPart):
    pass


def create_stored_chunk_upload_part_type(
    store_name: str,
    store_security: StoreSecurityABC,
    chunk_upload_store_meta: StoreMeta,
):
    store_type = store_name.title().replace("_", "")
    # noinspection PyDataclass
    base_type = type(
        f"{store_type}UploadPart",
        (ChunkUploadPart,),
        {
            "stream_id": dataclasses.replace(
                ChunkUploadPart.stream_id,
                create_generator=ChunkUploadPartStreamIdGenerator(
                    chunk_upload_store_meta
                ),
            )
        },
    )
    result = stored(
        base_type,
        indexes=(
            AttrIndex("part_id"),
            PartitionSortIndex("part_id", "sort_key"),
            UniqueIndex(("part_id", "sort_key")),
            AttrIndex("stream_id"),
            PartitionSortIndex("stream_id", "sort_key"),
            UniqueIndex(("stream_id", "sort_key")),
        ),
        store_security=NO_ACCESS,
    )
    return result
