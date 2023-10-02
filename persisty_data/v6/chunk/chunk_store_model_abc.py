from abc import ABC

from persisty.finder.stored_finder_abc import find_stored_by_name

from persisty_data.v6.model.store_model_abc import StoreModelABC


class ChunkStoreModelABC(StoreModelABC, ABC):
    @classmethod
    def get_chunk_store_meta(cls):
        chunk_store_meta = getattr(cls, "__chunk_store_meta__", None)
        if not chunk_store_meta:
            chunk_store_meta = find_stored_by_name(cls.base_store_name() + "_chunk")
        return chunk_store_meta
