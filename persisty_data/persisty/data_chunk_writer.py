from dataclasses import dataclass, field
from io import RawIOBase
from types import TracebackType

from persisty.finder.store_meta_finder_abc import find_store_meta_by_name
from persisty.store.store_abc import StoreABC

from persisty_data.persisty.data_chunk import DataChunk, get_sort_key
from persisty_data.persisty.persisty_upload_part import PersistyUploadPart


@dataclass
class DataChunkWriter(RawIOBase):
    upload_part: PersistyUploadPart
    data_chunk_store: StoreABC[DataChunk] = field(
        default_factory=lambda: find_store_meta_by_name("data_chunk")
    )
    chunk_size: int = 256 * 1024
    max_num_chunks: int = 1024 * 1024 * 64
    buffer: bytearray = field(default_factory=bytearray)
    chunk_number: int = 0

    def __enter__(self):
        pass

    def write(self, __b) -> int | None:
        offset = 0
        remaining_read = len(__b)
        remaining_write = self.chunk_size - len(self.buffer)
        while remaining_read:
            to_copy = min(remaining_read, remaining_write)
            self.buffer.extend(__b[offset : offset + to_copy])
            offset += to_copy
            remaining_read -= to_copy
            if not remaining_write:
                self._create_chunk()
        return offset

    def _create_chunk(self):
        data = bytes(self.buffer)
        data_chunk = DataChunk(
            upload_id=self.upload_part.upload_id,
            part_id=self.upload_part.id,
            part_number=self.upload_part.part_number,
            chunk_number=self.chunk_number,
            sort_key=get_sort_key(self.upload_part.part_number, self.chunk_number),
            data=data,
        )
        self.data_chunk_store.create(data_chunk)
        self.buffer.clear()
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
