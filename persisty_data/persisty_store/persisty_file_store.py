import hashlib
from dataclasses import field, dataclass
from io import IOBase
from typing import Optional, Iterator
from uuid import uuid4

from persisty.attr.attr_filter import attr_eq
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr
from persisty.store.store_abc import StoreABC
from persisty.store_meta import get_meta, StoreMeta

from persisty_data.file_handle import FileHandle
from persisty_data.persisty_store.data_chunk import DataChunk
from persisty_data.persisty_store.data_chunk_reader import DataChunkReader
from persisty_data.persisty_store.data_chunk_writer import DataChunkWriter
from persisty_data.persisty_store.persisty_file_handle import PersistyFileHandle
from persisty_data.persisty_store.persisty_file_handle_writer import (
    PersistyFileHandleWriter,
)
from persisty_data.persisty_store.persisty_file_store_abc import PersistyFileStoreABC
from persisty_data.persisty_store.persisty_upload_part import PersistyUploadPart


@dataclass
class PersistyFileStore(PersistyFileStoreABC):
    data_chunk_store: StoreABC[DataChunk] = field(
        default_factory=get_meta(DataChunk).create_store
    )

    def get_persisty_store_meta(self) -> Iterator[StoreMeta]:
        yield from super().get_persisty_store_meta()
        yield self.data_chunk_store.get_meta()

    def content_write(
        self,
        file_name: Optional[str],
        content_type: Optional[str] = None,
    ) -> IOBase:
        writer = PersistyFileHandleWriter(
            store_name=self.meta.name,
            file_name=file_name,
            upload_part=PersistyUploadPart(
                id=uuid4(), upload_id=uuid4(), part_number=0
            ),
            content_type=content_type,
        )
        return writer

    def upload_write(
        self,
        part_id: str,
    ) -> Optional[IOBase]:
        """Create a writer to the upload within the store"""
        upload_part = self.upload_part_store.read(part_id)
        if not upload_part:
            return
        self.data_chunk_store.delete_all(
            (
                attr_eq("upload_id", upload_part.upload_id)
                & attr_eq("part_number", upload_part.part_number)
            )
        )
        writer = DataChunkWriter(upload_part=upload_part)
        return writer

    def content_read(self, file_name: str) -> Optional[IOBase]:
        file_handle = self.file_handle_store.read(self._to_key(file_name))
        if file_handle:
            chunks = self.data_chunk_store.search_all(
                attr_eq("upload_id", file_handle.upload_id),
                SearchOrder((SearchOrderAttr("sort_key"),)),
            )
            result = DataChunkReader(chunks)
            return result

    def file_delete(self, file_name: str) -> bool:
        key = self._to_key(file_name)
        file_handle = self.file_handle_store.read(key)
        if not file_handle:
            return False
        # pylint: disable=W0212
        # noinspection PyProtectedMember
        result = self.file_handle_store._delete(key, file_handle)
        if result:
            self.data_chunk_store.delete_all(
                attr_eq("upload_id", file_handle.upload_id)
            )
        return result

    def upload_finish(self, upload_id: str) -> Optional[FileHandle]:
        upload_handle = self.upload_handle_store.read(upload_id)
        if not upload_handle or upload_handle.store_name != self.meta.name:
            return
        file_handle_id = f"{self.meta.name}/{upload_handle.file_name}"
        file_handle = self.file_handle_store.read(file_handle_id)
        md5 = hashlib.md5()
        size_in_bytes = 0
        data_chunks = self.data_chunk_store.search_all(
            attr_eq("upload_id", upload_id),
            SearchOrder((SearchOrderAttr("sort_key"),)),
        )
        for data_chunk in data_chunks:
            md5.update(data_chunk.data)
            size_in_bytes += len(data_chunk.data)
        new_file_handle = PersistyFileHandle(
            id=file_handle_id,
            store_name=self.meta.name,
            file_name=upload_handle.file_name,
            upload_id=upload_handle.id,
            content_type=upload_handle.content_type,
            etag=md5.hexdigest(),
            size_in_bytes=size_in_bytes,
        )
        self.upload_handle_store.delete(str(upload_id))
        if file_handle:
            old_upload_id = file_handle.upload_id
            # pylint: disable=W0212
            # noinspection PyProtectedMember
            file_handle = self.file_handle_store._update(
                file_handle_id, file_handle, new_file_handle
            )
            self.data_chunk_store.delete_all(attr_eq("upload_id", str(old_upload_id)))
        else:
            file_handle = self.file_handle_store.create(new_file_handle)
        self.upload_part_store.delete_all(attr_eq("upload_id", upload_id))
        return self._to_file_handle(file_handle)

    def upload_delete(self, upload_id: str) -> bool:
        result = self.upload_handle_store.delete(upload_id)
        if result:
            self.upload_part_store.delete_all(attr_eq("upload_id", upload_id))
            self.data_chunk_store.delete_all(attr_eq("upload_id", upload_id))
        return result
