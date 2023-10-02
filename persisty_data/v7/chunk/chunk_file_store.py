from dataclasses import dataclass
from io import IOBase
from typing import Optional, List, Iterator
from uuid import UUID

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.result_set import ResultSet
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr
from persisty.security.store_access import StoreAccess
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta
from persisty.util import UNDEFINED
from servey.security.authorization import Authorization

from persisty_data.v7.chunk.chunk import Chunk
from persisty_data.v7.chunk.chunk_file_handle import ChunkFileHandle
from persisty_data.v7.chunk.chunk_upload import ChunkUpload
from persisty_data.v7.chunk.chunk_upload_part import ChunkUploadPart
from persisty_data.v7.file_store_abc import FileStoreABC
from persisty_data.v7.upload_handle import UploadHandle


@dataclass
class ChunkFileStore(FileStoreABC):
    meta: FileStoreMeta
    file_handle_store: StoreABC[ChunkFileHandle]
    upload_store: StoreABC[ChunkUpload]
    upload_part_store: StoreABC[ChunkUploadPart]
    chunk_store: StoreABC[Chunk]

    def get_name(self) -> str:
        return self.name

    def get_access(self, authorization: Optional[Authorization] = None) -> StoreAccess:
        return self.store_access

    def get_api_access(self):
        """ Get the potential access level for this store """

    def get_secured(self, authorization: Optional[Authorization] = None) -> DataStoreABC:
        pass

    def get_stored(self) -> Iterator[StoreMeta]:
        yield from (
            self.chunk_store,
            self.upload_part_store,
            self.upload_store,
            self.file_handle_store
        )

    def get_routes(self) -> Iterator[ROUTE]:
        # upload part update
        # content read

    def content_create(self, source: IOBase, key: Optional[str], content_type: Optional[str]):
        pass

    def content_read(self, source: IOBase, key: Optional[str], content_type: Optional[str]):
        pass

    def file_read(self, key: str) -> DataHandle:
        chunk_file_handle self.d

    def file_read_batch(self, keys: List[str]) -> List[Optional[DataHandle]]:
        pass

    def file_search(self, group_id__eq: Optional[str] = UNDEFINED) -> ResultSet[DataHandle]:
        pass

    def file_delete(self, key: str, authorization: Optional[Authorization] = None) -> bool:
        pass

    def upload_create(self, key: str, group_id: Optional[str], content_type: Optional[str],
                      size_in_bytes: Optional[int]) -> UploadHandle:
        pass

    def upload_read(self, upload_id: UUID) -> UploadHandle:
        pass

    def upload_search(self, group_id__eq: Optional[str] = UNDEFINED, page_key: Optional[str] = None,
                      limit: Optional[int] = None) -> ResultSet[UploadHandle]:
        pass

    def upload_count(self, group_id__eq: Optional[str] = UNDEFINED) -> int:
        pass

    def upload_finish(self, upload_id: str) -> DataHandle:
        pass

    def upload_delete(self, upload_id: str) -> bool:
        pass

    def upload_part_create(self, upload_id: str) -> Optional[str]:
        pass

    def upload_part_search(self, upload_id__eq: str, page_key: Optional[str] = None) -> ResultSet[str]:
        search_filter = AttrFilter('upload_id', AttrFilterOp.eq, upload_id__eq)
        search_order = SearchOrder((SearchOrderAttr('part_number'),))
        result_set = self.upload_part_store.search(search_filter, search_order, page_key)
        result = ResultSet(
            results=[r.upload_url for r in result_set.results],
            next_page_key=result_set.next_page_key
        )
        return result

    def upload_part_count(self, upload_id__eq: str) -> int:
        search_filter = AttrFilter('upload_id', AttrFilterOp.eq, upload_id__eq)
        result = self.upload_part_store.count(search_filter)
        return result