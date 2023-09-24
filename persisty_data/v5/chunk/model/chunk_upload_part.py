from datetime import datetime
from typing import Optional
from uuid import UUID

from persisty.attr.attr import Attr
from persisty.index.attr_index import AttrIndex
from persisty.result_set import ResultSet
from persisty.security.store_access import NO_UPDATES
from persisty.security.store_security import INTERNAL_ONLY
from persisty.store_meta import get_meta
from persisty.stored import stored

from persisty_data.v5.upload_part import UploadPartStored, UploadPart


class ChunkUploadPart(UploadPart):
    stream_id: Optional[UUID]


ChunkUploadPartStored = stored(ChunkUploadPart)


def chunk_upload_part(upload_part: UploadPart, stream_id: UUID) -> ChunkUploadPart:
    result = ChunkUploadPartStored(**{
        a.name: getattr(upload_part, a.name)
        for a in get_meta(UploadPart).attrs
    })
    result.stream_id = stream_id
    return result
