import dataclasses
from io import IOBase
from typing import Iterator, Optional, List

from persisty.errors import PersistyError
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.security.store_access import StoreAccess
from persisty.store_meta import StoreMeta, get_meta
from persisty.util.encrypt_at_rest import decrypt, encrypt

from persisty_data.file_handle import FileHandle
from persisty_data.file_store_abc import FileStoreABC, _Route
from persisty_data.file_store_meta import FileStoreMeta
from persisty_data.stored_file_handle import (
    FileHandleSearchOrder,
    FileHandleResultSet,
    StoredFileHandle,
)
from persisty_data.upload_handle import UploadHandleResultSet, UploadHandle
from persisty_data.upload_part import UploadPartResultSet, UploadPart


@dataclasses.dataclass
class RestrictAccessFileStore(FileStoreABC):
    file_store: FileStoreABC
    store_access: StoreAccess

    def get_meta(self) -> FileStoreMeta:
        store_meta = getattr(self, "_store_meta", None)
        if not store_meta:
            store_meta = self.file_store.get_meta()
            store_meta = dataclasses.replace(
                store_meta,
                store_access=self.store_access & store_meta.store_access,
            )
            setattr(self, "_store_meta", store_meta)
        return store_meta

    def get_persisty_store_meta(self) -> Iterator[StoreMeta]:
        return self.file_store.get_persisty_store_meta()

    def get_routes(self) -> Iterator[_Route]:
        return self.file_store.get_routes()

    def content_read(self, file_name: str) -> Optional[IOBase]:
        read_filter = self.store_access.read_filter
        if read_filter is EXCLUDE_ALL:
            return None
        file_handle = self.file_read(file_name)
        if read_filter.match(file_handle, get_meta(StoredFileHandle).attrs):
            return self.file_store.content_read(file_name)

    def content_write(
        self, file_name: Optional[str], content_type: Optional[str]
    ) -> IOBase:
        store_access = self.store_access
        create_filter = store_access.create_filter
        update_filter = store_access.update_filter
        if create_filter is INCLUDE_ALL and update_filter is INCLUDE_ALL:
            return self.content_write(file_name, content_type)
        if create_filter is EXCLUDE_ALL and update_filter is EXCLUDE_ALL:
            raise PersistyError("forbidden")
        attrs = get_meta(StoredFileHandle).attrs
        file_handle = self.file_read(file_name)
        if file_handle:
            if not store_access.update_filter.match(file_handle, attrs):
                raise PersistyError("forbidden")
        else:
            file_handle = StoredFileHandle(
                file_name=file_name, content_type=content_type
            )
            if not store_access.create_filter.match(file_handle, attrs):
                raise PersistyError("forbidden")
        return self.file_store.content_write(file_name, content_type)

    def upload_write(self, part_id: str) -> Optional[IOBase]:
        # Security already handled by part_create methods
        return self.file_store.upload_write(part_id)

    def file_read(self, file_name: str) -> Optional[FileHandle]:
        if self.store_access.read_filter is EXCLUDE_ALL:
            return
        attrs = get_meta(StoredFileHandle).attrs
        file_handle = self.file_store.file_read(file_name)
        if self.store_access.read_filter.match(file_handle, attrs):
            return file_handle

    def file_read_batch(self, file_names: List[str]) -> List[Optional[FileHandle]]:
        read_filter = self.store_access.read_filter
        if read_filter is EXCLUDE_ALL:
            return [None for _ in file_names]
        attrs = get_meta(StoredFileHandle).attrs
        results = self.file_store.file_read_batch(file_names)
        results = [r if read_filter.match(r, attrs) else None for r in results]
        return results

    def file_search(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[FileHandleSearchOrder] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> FileHandleResultSet:
        store_access = self.store_access
        search_filter &= store_access.read_filter
        if not store_access.searchable or search_filter is EXCLUDE_ALL:
            return FileHandleResultSet([])
        return self.file_store.file_search(search_filter, search_order, page_key, limit)

    def file_count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        store_access = self.store_access
        search_filter &= store_access.read_filter
        if not store_access.searchable or search_filter is EXCLUDE_ALL:
            return 0
        return self.file_store.file_count(search_filter)

    def file_delete(self, file_name: str) -> bool:
        store_access = self.store_access
        if store_access.delete_filter is EXCLUDE_ALL:
            return False
        file_handle = self.file_store.file_read(file_name)
        attrs = get_meta(StoredFileHandle).attrs
        if not file_handle or not store_access.delete_filter.match(file_handle, attrs):
            return False
        return self.file_store.file_delete(file_name)

    def upload_create(
        self, file_name: str, content_type: Optional[str], size_in_bytes: Optional[int]
    ) -> Optional[UploadHandle]:
        file_store = self.file_store
        store_access = self.store_access
        file_handle = file_store.file_read(file_name)
        attrs = get_meta(StoredFileHandle).attrs
        if file_handle:
            if not store_access.update_filter.match(file_handle, attrs):
                return
            file_handle.content_type = content_type
            if size_in_bytes:
                file_handle.size_in_bytes = size_in_bytes
            if not store_access.update_filter.match(file_handle, attrs):
                return
        else:
            file_handle = StoredFileHandle(
                file_name=file_name,
                content_type=content_type,
                size_in_bytes=size_in_bytes,
            )
            if not store_access.create_filter.match(file_handle, attrs):
                return
        return self.file_store.upload_create(file_name, content_type, size_in_bytes)

    def upload_read(self, upload_id: str) -> Optional[UploadHandle]:
        create_filter = self.store_access.create_filter
        update_filter = self.store_access.update_filter
        if create_filter is EXCLUDE_ALL and update_filter is EXCLUDE_ALL:
            return
        upload_handle = self.file_store.upload_read(upload_id)
        return self._filter_upload_handle(upload_handle)

    def _filter_upload_handle(self, upload_handle: Optional[UploadHandle]):
        create_filter = self.store_access.create_filter
        update_filter = self.store_access.update_filter
        if not upload_handle:
            return
        attrs = get_meta(StoredFileHandle).attrs
        file_handle = self.file_store.file_read(upload_handle.file_name)
        if file_handle:
            return upload_handle if update_filter.match(file_handle, attrs) else None
        file_handle = StoredFileHandle(
            file_name=upload_handle.file_name, content_type=upload_handle.content_type
        )
        if create_filter.match(file_handle):
            return upload_handle

    def upload_search(
        self, page_key: Optional[str] = None, limit: Optional[int] = None
    ) -> UploadHandleResultSet:
        create_filter = self.store_access.create_filter
        update_filter = self.store_access.update_filter
        if create_filter is EXCLUDE_ALL and update_filter is EXCLUDE_ALL:
            return UploadHandleResultSet([])
        if not limit:
            limit = self.get_meta().batch_size
        if page_key:
            page_key, skip = decrypt(page_key)
        else:
            skip = 0
        results = []
        while True:
            page = self.file_store.upload_search(page_key, limit)
            result_iter = (r for r in page.results if self._filter_upload_handle(r))
            offset = 0
            for result in result_iter:
                offset += 1
                if skip:
                    skip -= 1
                else:
                    results.append(result)
                    if len(results) == limit:
                        if not next(result_iter, None):
                            page_key = page.next_page_key
                            skip = 0
                        if page_key:
                            page_key = encrypt([page_key, skip])
                        return UploadHandleResultSet(results, page_key)
            if page.next_page_key:
                page_key = page.next_page_key
            else:
                return UploadHandleResultSet(results)

    def upload_count(self) -> int:
        if (
            self.store_access.create_filter is EXCLUDE_ALL
            and self.store_access.update_filter is EXCLUDE_ALL
        ):
            return UploadHandleResultSet([])
        count = 0
        page_key = None
        while True:
            page = self.file_store.upload_search(page_key)
            count += sum(1 for r in page.results if self._filter_upload_handle(r))
            page_key = page.next_page_key
            if not page_key:
                return count

    def upload_finish(self, upload_id: str) -> Optional[FileHandle]:
        upload_handle = self.upload_read(upload_id)
        if upload_handle:
            return self.file_store.upload_finish(upload_id)

    def upload_delete(self, upload_id: str) -> bool:
        upload_handle = self.upload_read(upload_id)
        if upload_handle:
            return self.file_store.upload_delete(upload_id)

    def upload_part_create(self, upload_id: str) -> Optional[UploadPart]:
        upload_handle = self.upload_read(upload_id)
        if upload_handle:
            return self.file_store.upload_part_create(upload_id)

    def upload_part_search(
        self,
        upload_id__eq: str,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> UploadPartResultSet:
        upload_handle = self.upload_read(upload_id__eq)
        if upload_handle:
            return self.file_store.upload_part_search(upload_id__eq, page_key, limit)

    def upload_part_count(self, upload_id__eq: str) -> int:
        upload_handle = self.upload_read(upload_id__eq)
        if upload_handle:
            return self.file_store.upload_part_count(upload_id__eq)
