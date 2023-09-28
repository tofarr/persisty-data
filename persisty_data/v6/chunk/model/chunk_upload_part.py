from typing import AsyncIterator
from uuid import uuid4

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.finder.stored_finder_abc import find_stored_by_name
from persisty.store_meta import get_meta

from persisty_data.v6.model.upload_part import UploadPart


class ChunkUploadPart(UploadPart):
    """
    Creatable but not updatable or deletable
    """

    async def save_content(self, content: AsyncIterator[bytes]):
        chunk_store_meta = self.get_chunk_store_meta()
        chunk_store = chunk_store_meta.create_store()
        chunk_store.deleta_all(AttrFilter("part_id", AttrFilterOp.eq, str(self.id)))
        chunk_type = chunk_store_meta.get_create_dataclass()
        chunk_number = 0
        size_in_bytes = 0
        async for data in content:
            chunk_number += 1
            chunk = chunk_type(
                id=uuid4(),
                upload_id=self.upload_id,
                part_id=str(self.id),
                part_number=self.part_number,
                chunk_number=chunk_number,
                sort_key=self.part_number * 1024 * 1024 + chunk_number,
                data=data,
            )
            chunk_store.create(chunk)
            size_in_bytes += len(data)
        return size_in_bytes

    @classmethod
    def get_chunk_store_meta(cls):
        chunk_store_meta = getattr(cls, "__chunk_store_meta__", None)
        if not chunk_store_meta:
            chunk_store_meta = find_stored_by_name(cls.base_store_name() + "_chunk")
        return chunk_store_meta

    @classmethod
    def base_store_name(cls):
        result = get_meta(cls).name[:-6]
        return result
