import hashlib
import math
import mimetypes
from dataclasses import field
from datetime import datetime
from io import IOBase
from typing import Optional, List, Iterator
from uuid import UUID

from dateutil.relativedelta import relativedelta
from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.batch_edit import BatchEdit
from persisty.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr
from persisty.store.store_abc import StoreABC
from persisty.store_meta import get_meta, StoreMeta

from persisty_data.v7.file_handle import FileHandle, StoredFileHandle, FileHandleSearchFilter, FileHandleSearchOrder, \
    FileHandleResultSet
from persisty_data.v7.file_store_abc import FileStoreABC, _Route
from persisty_data.v7.file_store_meta import FileStoreMeta
from persisty_data.v7.persisty.data_chunk import DataChunk
from persisty_data.v7.persisty.persisty_file_handle import PersistyFileHandle
from persisty_data.v7.persisty.persisty_upload_handle import PersistyUploadHandle
from persisty_data.v7.persisty.persisty_upload_part import PersistyUploadPart
from persisty_data.v7.upload_handle import UploadHandle, UploadHandleResultSet


class PersistyFileStore(FileStoreABC):
    meta: FileStoreMeta
    file_handle_store: StoreABC[PersistyFileHandle] = field(default_factory=get_meta(PersistyFileHandle).create_store)
    upload_handle_store: StoreABC[PersistyUploadHandle] = field(default_factory=get_meta(PersistyUploadHandle).create_store)
    upload_part_store: StoreABC[PersistyUploadPart] = field(default_factory=get_meta(PersistyUploadPart).create_store)
    data_chunk_store: StoreABC[DataChunk] = field(default_factory=get_meta(DataChunk).create_store)
    download_url_pattern: str = "/data/{store_name}/{file_name}"
    upload_url_pattern: str = "/data/{store_name}/{upload_id}/{part_number}"

    def get_meta(self):
        return self.meta

    def get_persisty_store_meta(self) -> Iterator[StoreMeta]:
        yield from (
            self.file_handle_store.get_meta(),
            self.upload_handle_store.get_meta(),
            self.upload_part_store.get_meta(),
            self.data_chunk_store.get_meta(),
        )

    def get_routes(self) -> Iterator[_Route]:
        pass

    def content_create(self, source: IOBase, file_name: Optional[str], content_type: Optional[str]):
        pass

    def content_read(self, source: IOBase, file_name: Optional[str], content_type: Optional[str]):
        pass

    def file_read(self, file_name: str) -> FileHandle:
        result = self.file_handle_store.read(f"{self.meta.name}/{file_name}")
        result = self._to_file_handle(result)
        return result

    def _to_file_handle(self, file_handle: Optional[PersistyFileHandle]) -> Optional[FileHandle]:
        if file_handle:
            return StoredFileHandle(
                file_name=file_handle.file_name,
                content_type=file_handle.content_type,
                etag=file_handle.etag,
                size_in_bytes=file_handle.size_in_bytes,
                download_url=self.download_url_pattern.format(
                    store_name=self.meta.name,
                    file_name=file_handle.file_name,
                ),
                updated_at=file_handle.updated_at,
            )

    def file_read_batch(self, file_names: List[str]) -> List[Optional[FileHandle]]:
        assert len(file_names) <= self.meta.batch_size
        file_names = [f"{self.meta.name}/{file_name}" for file_name in file_names]
        results = self.file_handle_store.read_batch(file_names)
        results = [self._to_file_handle(result) for result in results]
        return results

    def file_search(
        self,
        search_filter: Optional[FileHandleSearchFilter] = None,
        search_order: Optional[FileHandleSearchOrder] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None
    ) -> FileHandleResultSet:
        search_filter = search_filter.to_search_filter() if search_filter else INCLUDE_ALL
        search_order = search_order.to_search_order() if search_order else None
        result_set = self.file_handle_store.search(search_filter, search_order, page_key, limit)
        result = FileHandleResultSet(
            results=[self._to_file_handle(r) for r in result_set.results],
            next_page_key=result_set.next_page_key
        )
        return result

    def file_count(self, search_filter: Optional[FileHandleSearchFilter] = None) -> int:
        search_filter = search_filter.to_search_filter() if search_filter else INCLUDE_ALL
        result = self.file_handle_store.count(search_filter)
        return result

    def file_delete(self, file_name: str) -> bool:
        key = f"{self.meta.name}/{file_name}"
        file_handle = self.file_handle_store.read(key)
        if not file_handle:
            return False
        # noinspection PyProtectedMember
        result = self.file_handle_store._delete(key, file_handle)
        if result:
            self.data_chunk_store.delete_all(AttrFilter('upload_id', AttrFilterOp.eq, file_handle.upload_id))
        return result

    def upload_create(
        self,
        file_name: str,
        content_type: Optional[str],
        size_in_bytes: Optional[int]
    ) -> UploadHandle:
        if not content_type:
            value = mimetypes.guess_type(file_name)[0]
        upload_handle = PersistyUploadHandle(
            store_name=self.meta.name,
            file_name=file_name,
            content_type=content_type,
            expire_at=datetime.now() + relativedelta(seconds=self.meta.upload_expire_in)
        )
        upload_handle = self.upload_handle_store.create(upload_handle)
        number_of_parts = 1
        if size_in_bytes:
            assert size_in_bytes <= self.meta.max_file_size
            number_of_parts = math.ceil(size_in_bytes / self.meta.max_part_size)
        edits = (
            BatchEdit(create_item=PersistyUploadPart(
                upload_id=upload_handle.id,
                part_number=part_number
            ))
            for part_number in range(1, number_of_parts + 1)
        )
        upload_parts = self.upload_part_store.edit_all(edits)
        sum(1 for _ in upload_parts)
        result = UploadHandle(
            id=upload_handle.id,
            store_name=upload_handle.store_name,
            file_name=file_name,
            content_type=upload_handle.content_type,
            expire_at=upload_handle.expire_at,
        )
        return result

    def upload_read(self, upload_id: UUID) -> Optional[UploadHandle]:
        upload_handle = self.upload_handle_store.read(str(upload_id))
        if upload_handle:
            return self._to_upload_handle(upload_handle)

    def _to_upload_handle(self, upload_handle: Optional[PersistyUploadHandle]) -> Optional[UploadHandle]:
        if upload_handle:
            return UploadHandle(
                id=upload_handle.id,
                store_name=self.meta.name,
                file_name=upload_handle.file_name,
                content_type=upload_handle.content_type,
                expire_at=upload_handle.expire_at
            )

    def upload_search(self, page_key: Optional[str] = None, limit: Optional[int] = None) -> UploadHandleResultSet
        result = self.upload_handle_store.search(
            AttrFilter('store_name', AttrFilterOp.eq, self.meta.name),
            SearchOrder((SearchOrderAttr("file_name"),)),
            page_key,
            limit
        )
        return result

    def upload_count(self) -> int:
        result = self.upload_handle_store.count(
            AttrFilter('store_name', AttrFilterOp.eq, self.meta.name)
        )
        return result

    def upload_finish(self, upload_id: UUID) -> Optional[FileHandle]:
        upload_handle = self.upload_handle_store.read(str(upload_id))
        if not upload_handle or upload_handle.store_name != self.meta.name:
            return
        file_handle_id = f"{self.meta.name}/{upload_handle.store_name}"
        file_handle = self.file_handle_store.read(str(upload_id))
        md5 = hashlib.md5()
        size_in_bytes = 0
        data_chunks = self.data_chunk_store.search_all(
            AttrFilter("upload_id", AttrFilterOp.eq, str(upload_id)),
            SearchOrder((SearchOrderAttr("sort_key)"),))
        )
        for data_chunk in data_chunks:
            md5.update(data_chunk.data)
            size_in_bytes += len(data_chunk.data)
        new_file_handle = PersistyFileHandle(
            id=file_handle_id,
            file_name=upload_handle.file_name,
            upload_id=upload_handle.id,
            content_type=upload_handle.content_type,
            etag=md5.hexdigest(),
            size_in_bytes=size_in_bytes,
        )
        self.upload_handle_store.delete(str(upload_id))
        if file_handle:
            old_upload_id = file_handle.upload_id
            # noinspection PyProtectedMember
            file_handle = self.file_handle_store._update(file_handle_id, file_handle, new_file_handle)
            self.data_chunk_store.delete_all(AttrFilter('upload_id', AttrFilterOp.eq, old_upload_id))
        else:
            file_handle = self.file_handle_store.create(new_file_handle)
        return self._to_file_handle(file_handle)

    def upload_delete(self, upload_id: UUID) -> bool:
        pass

    def upload_part_create(self, upload_id: UUID) -> Optional[str]:
        pass

    def upload_part_search(self, upload_id__eq: UUID, page_key: Optional[str] = None) -> ResultSet[str]:
        pass

    def upload_part_count(self, upload_id__eq: UUID) -> int:
        pass
