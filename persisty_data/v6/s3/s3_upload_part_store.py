from typing import Optional

from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store.store_abc import StoreABC
from persisty.store_meta import T, StoreMeta

from persisty_data.v6.model.upload_part import UploadPart


class S3UploadPartStore(StoreABC[UploadPart]):
    bucket_name: str

    def get_meta(self) -> StoreMeta:
        pass

    def create(self, item: T) -> Optional[T]:
        pass

    def read(self, key: str) -> Optional[T]:
        pass

    def _update(self, key: str, item: T, updates: T) -> Optional[T]:
        raise NotImplementedError()

    def _delete(self, key: str, item: T) -> bool:
        pass

    def count(self, search_filter: SearchFilterABC[T] = INCLUDE_ALL) -> int:
        pass