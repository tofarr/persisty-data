import hashlib
from typing import Optional

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr
from persisty.store_meta import get_meta
from servey.security.authorization import Authorization

from persisty_data.v6.chunk.chunk_file_handle import ChunkFileHandle
from persisty_data.v6.chunk.chunk_store_model_abc import ChunkStoreModelABC
from persisty_data.v6.model.upload import Upload


class ChunkUpload(Upload, ChunkStoreModelABC):
    """
    Creatable but not updatable
    """

    def finish_upload(self, authorization: Optional[Authorization]) -> ChunkFileHandle:
        chunk_store_meta = self.get_chunk_store_meta()
        chunk_store = chunk_store_meta.create_store()
        search_filter = AttrFilter("upload_id", AttrFilterOp.eq, self.id)
        search_order = SearchOrder((SearchOrderAttr("sort_key"),))
        chunks = chunk_store.search_all(search_filter, search_order)
        md5 = hashlib.md5()
        size_in_bytes = 0
        for chunk in chunks:
            size_in_bytes += len(chunk.data)
            md5.update(chunk.data)

        file_handle_store_meta = self.get_file_handle_store_meta()
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

    def after_delete(self):
        file_handle_store = self.get_file_handle_store_meta().create_store()
        search_filter = AttrFilter("upload_id", AttrFilterOp.eq, str(self.id))
        file_handle_result_set = file_handle_store.search(search_filter)
        if not file_handle_result_set.results:
            chunk_store = self.get_chunk_store_meta().create_store()
            chunk_store.delete_all(search_filter)
        upload_part_store_meta = self.get_upload_part_store_meta()
        upload_part_store_meta.create_store().delete_all(search_filter)

    @classmethod
    def base_store_name(cls):
        result = get_meta(cls).name[:-7]
        return result
