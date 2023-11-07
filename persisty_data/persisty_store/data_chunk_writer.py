from dataclasses import dataclass, field
from io import RawIOBase
from types import TracebackType

from persisty.finder.store_meta_finder_abc import find_store_meta_by_name
from persisty.store.store_abc import StoreABC

from persisty_data.persisty_store.data_chunk import DataChunk, get_sort_key
from persisty_data.persisty_store.persisty_upload_part import PersistyUploadPart


@dataclass
class DataChunkWriter(RawIOBase):
    upload_part: PersistyUploadPart
    data_chunk_store: StoreABC[DataChunk] = field(
        default_factory=lambda: find_store_meta_by_name("data_chunk").create_store()
    )
    chunk_size: int = 256 * 1024
    max_num_chunks: int = 1024 * 1024 * 64
    buffer: bytearray = field(default_factory=bytearray)
    chunk_number: int = 0

    def __enter__(self):
        return self

    def write(self, __b) -> int | None:
        src_offset = 0
        while src_offset < len(__b):
            num_bytes_to_copy = min(
                self.chunk_size - len(self.buffer),
                len(__b) - src_offset
            )
            self.buffer.extend(__b[src_offset: src_offset + num_bytes_to_copy])
            src_offset += num_bytes_to_copy
            if len(self.buffer) == self.chunk_size:
                self._create_chunk()
        return src_offset

    def _create_chunk(self):
        data = bytes(self.buffer)
        data_chunk = DataChunk(
            upload_id=str(self.upload_part.upload_id),
            part_number=self.upload_part.part_number,
            chunk_number=self.chunk_number,
            sort_key=get_sort_key(self.upload_part.part_number, self.chunk_number),
            data=data,
        )
        self.data_chunk_store.create(data_chunk)
        self.buffer = bytearray()
        self.chunk_number += 1
        assert self.chunk_number <= self.max_num_chunks

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self.buffer:
            self._create_chunk()
