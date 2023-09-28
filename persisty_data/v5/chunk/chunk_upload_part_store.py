from dataclasses import dataclass
from typing import Optional

from celery.result import ResultSet
from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.errors import PersistyError
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta, get_meta

from persisty_data.v5.chunk.model.chunk_upload import ChunkUpload
from persisty_data.v5.chunk.model.chunk_upload_part import (
    ChunkUploadPart,
    to_upload_part,
)
from persisty_data.v5.upload_part import UploadPart, UploadPartStored


@dataclass
class ChunkUploadPartStore(StoreABC[UploadPart]):
    chunk_upload_part_store: StoreABC[ChunkUploadPart]
    chunk_upload_store: StoreABC[ChunkUpload]
    upload_url_pattern: str

    def get_meta(self) -> StoreMeta:
        return get_meta(UploadPartStored)

    def create(self, item: UploadPart) -> Optional[UploadPart]:
        upload = self.chunk_upload_store.read(str(item.upload_id))
        if not upload:
            return  # no part created
        existing_num_parts = self.chunk_upload_part_store.count(
            AttrFilter("upload_id", AttrFilterOp.eq, upload.id)
        )
        store_meta = self.chunk_upload_part_store.get_meta()
        chunk_upload_part = store_meta.get_create_dataclass()(
            id=item.id,
            item_key=upload.item_key,
            upload_id=item.upload_id,
            stream_id=upload.stream_id,
            part_number=existing_num_parts + 1,
        )
        chunk_upload_part = self.chunk_upload_part_store.create(chunk_upload_part)
        upload_part = to_upload_part(chunk_upload_part)
        return upload_part

    def read(self, key: str) -> Optional[UploadPart]:
        chunk_upload_part = self.chunk_upload_part_store.read(key)
        upload_part = to_upload_part(chunk_upload_part)
        return upload_part

    def _update(
        self, key: str, item: UploadPart, updates: UploadPart
    ) -> Optional[UploadPart]:
        raise PersistyError("not_supported")

    def _delete(self, key: str, item: UploadPart) -> bool:
        raise PersistyError("not_supported")

    def search(
        self,
        search_filter: SearchFilterABC[UploadPart] = INCLUDE_ALL,
        search_order: Optional[SearchOrder[UploadPart]] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[UploadPart]:
        # noinspection PyTypeChecker
        return self.chunk_upload_part_store.search(
            search_filter, search_order, page_key, limit
        )

    def count(self, search_filter: SearchFilterABC[UploadPart] = INCLUDE_ALL) -> int:
        # noinspection PyTypeChecker
        return self.chunk_upload_part_store.count(search_filter)
