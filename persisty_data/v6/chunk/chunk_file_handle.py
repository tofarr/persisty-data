from io import IOBase

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr
from persisty.store_meta import get_meta

from persisty_data.v6.chunk.chunk_reader import ChunkReader
from persisty_data.v6.chunk.chunk_store_model_abc import ChunkStoreModelABC
from persisty_data.v6.model.file_handle import FileHandle


class ChunkFileHandle(FileHandle, ChunkStoreModelABC):
    """
    File handle - not directly creatable or updatable
    """

    upload_id: str

    def get_reader(self) -> IOBase:
        chunk_store_meta = self.get_chunk_store_meta()
        chunk_store = chunk_store_meta.create_store()
        search_filter = AttrFilter("upload_id", AttrFilterOp.eq, self.upload_id)
        search_order = SearchOrder((SearchOrderAttr("sort_key"),))
        chunks = chunk_store.search_all(search_filter, search_order)
        return ChunkReader(chunks)

    def after_delete(self):
        self.get_chunk_store_meta().delete_all(
            AttrFilter("upload_id", AttrFilterOp.eq, self.upload_id)
        )

    @classmethod
    def base_store_name(cls):
        result = get_meta(cls).name
        return result
