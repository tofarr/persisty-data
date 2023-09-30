from dataclasses import dataclass
from typing import Optional, Dict, Callable

from persisty.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.store.store_abc import StoreABC
from persisty.store_meta import T, StoreMeta

from persisty_data.v6.s3.model.s3_file_handle import S3FileHandle
from persisty_data.v6.s3.s3_client import get_s3_client


@dataclass
class S3FileHandleStore(StoreABC[S3FileHandle]):
    meta: StoreMeta

    def get_meta(self) -> StoreMeta:
        return self.meta

    def create(self, item: S3FileHandle) -> Optional[S3FileHandle]:
        raise NotImplementedError()

    def read(self, key: str) -> Optional[S3FileHandle]:
        stored_class = self.meta.get_stored_dataclass()
        # noinspection PyUnresolvedReferences
        bucket_name = stored_class.get_bucket_name()
        try:
            response = get_s3_client().get_object(
                BucketName=bucket_name,
                Key=key
            )
        except:
            return
        item = self._file_handle(response, key, stored_class)
        return item

    @staticmethod
    def _file_handle(response: Dict, key: str, stored_class: Callable):
        item = stored_class(
            key=key,
            content_type=response.get('ContentType'),
            size_in_bytes=response['ContentLength'],
            etag=response['ETag'],
            # download_url=
            subject_id=(response.get('Metadata') or {}).get('subject_id'),
            expire_at=response['Expires'],
        )
        item.download_url = item.get_download_url()
        return item

    def _update(self, key: str, item: S3FileHandle, updates: S3FileHandle) -> Optional[S3FileHandle]:
        raise NotImplementedError

    def _delete(self, key: str, item: S3FileHandle) -> bool:
        pass

    def count(self, search_filter: SearchFilterABC[S3FileHandle] = INCLUDE_ALL) -> int:
        pass

    def search(
        self,
        search_filter: SearchFilterABC[S3FileHandle] = INCLUDE_ALL,
        search_order: Optional[SearchOrder[S3FileHandle]] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[S3FileHandle]:
        pass
