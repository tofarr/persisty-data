from abc import ABC, abstractmethod

from persisty.store.store_abc import StoreABC

from persisty_data.chunk import Chunk
from persisty_data.v3.model.downloadable import Downloadable
from persisty_data.v3.model.stream import Stream


class DataStoreABC(ABC):
    @abstractmethod
    def get_chunk_store(self) -> StoreABC[Chunk]:
        """Get the store for chunks"""

    @abstractmethod
    def get_stream_store(self) -> StoreABC[Stream]:
        """Get the store for streams"""

    def get_downloadable_store(self) -> StoreABC[Downloadable]:
        """Get the read only store for downloadables"""
