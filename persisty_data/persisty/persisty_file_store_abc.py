import math
import mimetypes
from abc import ABC
from dataclasses import field, dataclass
from datetime import datetime
from typing import Optional, List, Iterator
from uuid import uuid4

from dateutil.relativedelta import relativedelta
from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.batch_edit import BatchEdit
from persisty.result_set import ResultSet
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr
from persisty.store.store_abc import StoreABC
from persisty.store_meta import get_meta, StoreMeta
from servey.security.authorizer.authorizer_factory_abc import get_default_authorizer

from persisty_data.file_handle import FileHandle
from persisty_data.file_store_abc import FileStoreABC, _Route
from persisty_data.file_store_meta import FileStoreMeta
from persisty_data.persisty.persisty_file_handle import PersistyFileHandle
from persisty_data.persisty.persisty_upload_handle import PersistyUploadHandle
from persisty_data.persisty.persisty_upload_part import PersistyUploadPart
from persisty_data.routes import create_route_for_part_upload, create_route_for_download
from persisty_data.stored_file_handle import (
    FileHandleSearchOrder,
    StoredFileHandle,
)
from persisty_data.upload_handle import UploadHandle, UploadHandleResultSet
from persisty_data.upload_part import UploadPart, UploadPartResultSet


@dataclass
class PersistyFileStoreABC(FileStoreABC, ABC):
    meta: FileStoreMeta
    file_handle_store: StoreABC[PersistyFileHandle] = field(
        default_factory=get_meta(PersistyFileHandle).create_store
    )
    upload_handle_store: StoreABC[PersistyUploadHandle] = field(
        default_factory=get_meta(PersistyUploadHandle).create_store
    )
    upload_part_store: StoreABC[PersistyUploadPart] = field(
        default_factory=get_meta(PersistyUploadPart).create_store
    )
    download_url_pattern: str = "/data/{store_name}/{file_name}"
    upload_url_pattern: str = "/data/{store_name}/{part_id}"

    def get_meta(self):
        return self.meta

    def get_persisty_store_meta(self) -> Iterator[StoreMeta]:
        yield from (
            self.file_handle_store.get_meta(),
            self.upload_handle_store.get_meta(),
            self.upload_part_store.get_meta(),
        )

    def get_routes(self) -> Iterator[_Route]:
        authorizer = get_default_authorizer()
        api_store = self.get_meta().store_security.get_api(self)
        api_access = api_store.get_meta().store_access
        if api_access.read_filter is not EXCLUDE_ALL:
            yield create_route_for_part_upload(self, authorizer)
        if (
            api_access.create_filter is not EXCLUDE_ALL
            or api_access.update_filter is not EXCLUDE_ALL
        ):
            yield create_route_for_download(self, authorizer)

    def _to_key(self, file_name: str):
        return f"{self.meta.name}/{file_name}"

    def file_read(self, file_name: str) -> Optional[FileHandle]:
        result = self.file_handle_store.read(self._to_key(file_name))
        result = self._to_file_handle(result)
        return result

    def _to_file_handle(
        self, file_handle: Optional[PersistyFileHandle]
    ) -> Optional[FileHandle]:
        if file_handle:
            return StoredFileHandle(
                file_name=file_handle.file_name,
                content_type=file_handle.content_type,
                etag=file_handle.etag,
                size_in_bytes=file_handle.size_in_bytes,
                download_url=self.download_url_pattern.format(
                    store_name=self.meta.name.replace('_', '-'),
                    file_name=file_handle.file_name,
                ),
                updated_at=file_handle.updated_at,
            )

    def file_read_batch(self, file_names: List[str]) -> List[Optional[FileHandle]]:
        assert len(file_names) <= self.meta.batch_size
        file_names = [self._to_key(file_name) for file_name in file_names]
        results = self.file_handle_store.read_batch(file_names)
        results = [self._to_file_handle(result) for result in results]
        return results

    def file_search(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[FileHandleSearchOrder] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[FileHandle]:
        search_order = search_order.to_search_order() if search_order else None
        result_set = self.file_handle_store.search(
            search_filter, search_order, page_key, limit
        )
        result = ResultSet(
            results=[self._to_file_handle(r) for r in result_set.results],
            next_page_key=result_set.next_page_key,
        )
        return result

    def file_count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        result = self.file_handle_store.count(search_filter)
        return result

    def upload_create(
        self, file_name: Optional[str], content_type: Optional[str], size_in_bytes: Optional[int]
    ) -> UploadHandle:
        id_ = str(uuid4())
        if not file_name:
            file_name = id_
            if content_type:
                file_name = f"{id_}.{content_type.split('/')[-1]}"
        if not content_type:
            content_type = mimetypes.guess_type(file_name)[0]
        upload_handle = PersistyUploadHandle(
            id=id_,
            store_name=self.meta.name,
            file_name=file_name,
            content_type=content_type,
            expire_at=datetime.now()
            + relativedelta(seconds=self.meta.upload_expire_in),
        )
        upload_handle = self.upload_handle_store.create(upload_handle)
        number_of_parts = 1
        if size_in_bytes:
            assert size_in_bytes <= self.meta.max_file_size
            number_of_parts = math.ceil(size_in_bytes / self.meta.max_part_size)
        edits = (
            BatchEdit(
                create_item=PersistyUploadPart(
                    upload_id=upload_handle.id, part_number=part_number
                )
            )
            for part_number in range(number_of_parts)
        )
        upload_parts = self.upload_part_store.edit_all(edits)
        sum(1 for _ in upload_parts)
        result = UploadHandle(
            id=str(upload_handle.id),
            store_name=upload_handle.store_name,
            file_name=file_name,
            content_type=upload_handle.content_type,
            expire_at=upload_handle.expire_at,
        )
        return result

    def upload_read(self, upload_id: str) -> Optional[UploadHandle]:
        upload_handle = self.upload_handle_store.read(str(upload_id))
        if upload_handle:
            return self._to_upload_handle(upload_handle)

    def _to_upload_handle(
        self, upload_handle: Optional[PersistyUploadHandle]
    ) -> Optional[UploadHandle]:
        if upload_handle:
            return UploadHandle(
                id=str(upload_handle.id),
                store_name=self.meta.name,
                file_name=upload_handle.file_name,
                content_type=upload_handle.content_type,
                expire_at=upload_handle.expire_at,
            )

    def upload_search(
        self, page_key: Optional[str] = None, limit: Optional[int] = None
    ) -> UploadHandleResultSet:
        result = self.upload_handle_store.search(
            AttrFilter("store_name", AttrFilterOp.eq, self.meta.name),
            SearchOrder((SearchOrderAttr("file_name"),)),
            page_key,
            limit,
        )
        return result

    def upload_count(self) -> int:
        result = self.upload_handle_store.count(
            AttrFilter("store_name", AttrFilterOp.eq, self.meta.name)
        )
        return result

    def upload_part_create(self, upload_id: str) -> Optional[UploadPart]:
        upload_handle = self.upload_handle_store.read(upload_id)
        if upload_handle:
            upload_part = PersistyUploadPart(upload_id=upload_handle.id)
            upload_part = self.upload_part_store.create(upload_part)
            if upload_part:
                result = self._to_upload_part(upload_part, upload_handle)
                return result

    def _to_upload_part(
        self,
        upload_part: Optional[PersistyUploadPart],
        upload_handle: PersistyUploadHandle,
    ) -> Optional[UploadPart]:
        if upload_part:
            result = UploadPart(
                id=str(upload_part.id),
                upload_id=upload_part.upload_id,
                part_number=upload_part.part_number + 1,
                upload_url=self.upload_url_pattern.format(
                    store_name=self.meta.name.replace('_', '-'),
                    part_id=upload_part.id,
                ),
            )
            return result

    def upload_part_search(
        self,
        upload_id__eq: str,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> UploadPartResultSet:
        search_filter = AttrFilter("upload_id", AttrFilterOp.eq, upload_id__eq)
        upload_handle = self.upload_handle_store.read(upload_id__eq)
        result = self.upload_part_store.search(
            search_filter, SearchOrder((SearchOrderAttr("part_number"),)), page_key, limit
        )
        result = UploadPartResultSet(
            results=[self._to_upload_part(p, upload_handle) for p in result.results],
            next_page_key=result.next_page_key,
        )
        return result

    def upload_part_count(self, upload_id__eq: str) -> int:
        search_filter = AttrFilter("upload_id", AttrFilterOp.eq, upload_id__eq)
        result = self.upload_part_store.count(search_filter)
        return result
