import hashlib
from typing import Optional

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.finder.stored_finder_abc import find_stored_by_name
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr
from persisty.store_meta import get_meta
from servey.security.authorization import Authorization

from persisty_data.v6.chunk.model.chunk_file_handle import ChunkFileHandle
from persisty_data.v6.model.upload import Upload


class ChunkUpload(Upload):
    """
    Creatable but not updatable
    """

    def finish_upload(self, authorization: Optional[Authorization]) -> ChunkFileHandle:
        file_handle_store_meta = self.get_file_handle_store_meta()
        chunk_store_meta = self.get_chunk_store_meta()
        chunk_store = chunk_store_meta.create_store()

        search_filter = AttrFilter("upload_id", AttrFilterOp.eq, self.id)
        search_order = SearchOrder((SearchOrderAttr("part_number"),))
        chunks = chunk_store.search_all(search_filter, search_order)
        md5 = hashlib.md5()
        size_in_bytes = 0
        for chunk in chunks:
            size_in_bytes += len(chunk.data)
            md5.update(chunk.data)
        file_handle = file_handle_store_meta.get_create_dataclass()(
            key=self.item_key,
            content_type=self.content_type,
            size_in_bytes=size_in_bytes,
            etag=md5.hexdigest(),
            subject_id=authorization.subject_id if authorization else None,
            download_url="/data/"
            + file_handle_store_meta.name.replace("_", "-")
            + "/"
            + self.item_key,
            upload_id=str(self.id),
        )
        file_handle_store = file_handle_store_meta.create_store()
        file_handle = file_handle_store.create(file_handle)
        get_meta(self.__class__).create_store().delete(self.id)
        return file_handle

    @classmethod
    def get_file_handle_store_meta(cls):
        file_handle_store_meta = getattr(cls, "__file_handle_store_meta__", None)
        if not file_handle_store_meta:
            file_handle_store_meta = find_stored_by_name(cls.base_store_name())
        return file_handle_store_meta

    @classmethod
    def get_chunk_store_meta(cls):
        chunk_store_meta = getattr(cls, "__chunk_store_meta__", None)
        if not chunk_store_meta:
            chunk_store_meta = find_stored_by_name(cls.base_store_name() + "_chunk")
        return chunk_store_meta

    @classmethod
    def base_store_name(cls):
        result = get_meta(cls).name[:-7]
        return result
