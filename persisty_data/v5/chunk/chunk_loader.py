import math
from dataclasses import dataclass
from io import IOBase
from typing import Callable, Iterator, Optional
from uuid import UUID, uuid4

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.batch_edit import BatchEdit
from persisty.errors import PersistyError
from persisty.result import Result
from persisty.result_set import ResultSet
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr
from persisty.store.store_abc import StoreABC

from persisty_data.chunk import Chunk
from persisty_data.v5.begin_upload_result import BeginUploadResult
from persisty_data.v5.chunk.model.chunk_file_handle import ChunkFileHandle
from persisty_data.v5.chunk.model.chunk_upload import ChunkUpload
from persisty_data.v5.chunk.model.chunk_upload_part import ChunkUploadPart
from persisty_data.v5.file_handle import FileHandle
from persisty_data.v5.loader_abc import LoaderABC


@dataclass
class ChunkLoader(LoaderABC):
    file_handle_store: StoreABC[ChunkFileHandle]
    chunk_store: StoreABC[Chunk]
    chunk_upload_store: StoreABC[ChunkUpload]
    chunk_part_store: StoreABC[ChunkUploadPart]
    upload_url_pattern: str
    download_url_pattern: str
    chunk_size: int = 256 * 1024 # 256kb
    max_chunks: int = 1024  # 256Mb
    max_parts: int = 1024  # 256Gb
    upload_expire_in: int = 3600

    def copy_file_from_store(self, source_key: str, destination: IOBase):
        file_handle = self.file_handle_store.read(source_key)
        if not file_handle:
            raise PersistyError("not_found")
        search_filter = AttrFilter("stream_id", AttrFilterOp.eq, file_handle.stream_id)
        search_order = SearchOrder((SearchOrderAttr("sort_key"),))
        chunks = self.chunk_store.search_all(search_filter, search_order)
        for chunk in chunks:
            destination.write(chunk.data)

    def copy_file_to_part(self, source: Callable[[], IOBase], part_id: UUID):
        """Write data from the source given to the store under the key given"""
        upload_part = self.chunk_part_store.read(str(part_id))
        upload_part.stream_id = uuid4()
        edits = self._create_chunk_edits(source, upload_part)
        self.chunk_store.edit_all(edits)
        self.chunk_part_store.update(upload_part)

    def begin_upload(self, key: Optional[str], content_type: Optional[str], file_size: Optional[int]) -> BeginUploadResult:
        create_upload_type = self.chunk_upload_store.get_meta().get_create_dataclass()
        upload = create_upload_type(
            item_key=key,
            content_type=content_type,
            max_part_size_in_bytes=self.chunk_size * self.max_chunks,
            max_number_of_parts=self.max_parts
        )
        upload = self.chunk_upload_store.create(upload)
        self._create_initial_parts(upload, file_size)
        search_filter = AttrFilter("upload_id", AttrFilterOp.eq, upload.id)
        search_order = SearchOrder((SearchOrderAttr("part_number"),))
        initial_parts = self.chunk_part_store.search(search_filter, search_order)
        initial_parts = ResultSet(
            next_page_key=initial_parts.next_page_key,
            results=[
                Result(
                    key=str(r.id),
                    item=r.to_part(self.upload_url_pattern),
                    updatable=False,
                    deletable=False
                )
                for r in initial_parts.results
            ]
        )
        result = BeginUploadResult(
            upload=upload,
            initial_parts=initial_parts
        )
        return result

    def finish_upload(self, upload_id: UUID) -> Optional[FileHandle]:
        upload = self.chunk_upload_store.read(str(upload_id))
        file_handle = self.file_handle_store.read(upload.item_key)
        if file_handle:
            file_handle.stream_id = upload.stream_id
            self.file_handle_store.update(file_handle)
            self.chunk_upload_store.delete(str(upload_id))
            return file_handle.to_file_handle(self.download_url_pattern)

    def _create_chunk_edits(
        self, source: Callable[[], IOBase], upload_part: ChunkUploadPart
    ) -> Iterator[BatchEdit[Chunk]]:
        chunk_number = 0
        base_sort_key = upload_part.part_number * 1024 * 1024
        chunk_type = self.chunk_store.get_meta().get_create_dataclass()
        with source() as reader:
            data = reader.read(self.chunk_size)
            if not data:
                return
            if chunk_number > self.max_chunks:
                raise PersistyError('too_large')
            chunk = chunk_type(
                id=uuid4(),
                item_key=upload_part.item_key,
                upload_id=upload_part.upload_id,
                part_id=upload_part.id,
                part_number=upload_part.part_number,
                stream_id=upload_part.stream_id,
                chunk_number=chunk_number,
                sort_key=base_sort_key + chunk_number,
                data=data,
            )
            edit = BatchEdit(create_item=chunk)
            yield edit
            chunk_number += 1

    def _create_initial_parts(self, upload: ChunkUpload, file_size: Optional[int]):
        num_parts = math.ceil(file_size / (self.chunk_size * self.max_chunks))
        if num_parts >= self.max_parts:
            raise PersistyError('size_exceeded')
        part_numbers = range(1, num_parts + 1)
        part_type = self.chunk_part_store.get_meta().get_create_dataclass()
        edits = (
            BatchEdit(
                create_item=part_type(
                    item_key=upload.item_key,
                    upload_id=upload.id,
                    part_number=part_number
                )
            )
            for part_number in part_numbers
        )
        sum(1 for _ in self.chunk_part_store.edit_all(edits))
