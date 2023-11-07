import os
from dataclasses import dataclass
from io import RawIOBase
from typing import Iterator, Optional

from persisty.util import UNDEFINED

from persisty_data.persisty_store.data_chunk import DataChunk


@dataclass
class DataChunkReader(RawIOBase):
    chunks: Iterator[DataChunk]
    current_chunk: Optional[DataChunk] = UNDEFINED
    offset_: int = 0
    position_: int = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def readinto(self, output):
        if self.current_chunk is UNDEFINED:
            self.current_chunk = next(self.chunks, None)
        read_remaining = len(output)
        result = 0
        while True:
            if self.current_chunk is None:
                return result
            data = self.current_chunk.data
            length = min(read_remaining, len(data) - self.offset_)
            output[:length] = data[self.offset_ : (self.offset_ + length)]
            self.offset_ += length
            read_remaining -= length
            result += length
            self.position_ += length
            if not read_remaining:
                return result
            self.current_chunk = next(self.chunks, None)
            self.offset_ = 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def seek(self, __offset: int, __whence: int = os.SEEK_SET):
        if __whence == os.SEEK_SET:  # absolute positioning
            __whence = os.SEEK_CUR
            __offset -= self.position_
        if __whence != os.SEEK_CUR:
            raise NotImplementedError()  # we do not support seeking relative to the end of the file
        if __offset < 0:
            if __offset + self.offset_ < 0:
                # we do not support going back to previous chunks - in order to do this we would need
                # to support restarting the stream from the beginning
                raise NotImplementedError()
            self.offset_ += __offset
            self.position_ += __offset
            return self.position_
        while __offset:
            if self.current_chunk is UNDEFINED:
                self.current_chunk = next(self.chunks)
            remaining_bytes_in_chunk = len(self.current_chunk.data) - self.offset_
            if remaining_bytes_in_chunk < __offset:
                __offset -= remaining_bytes_in_chunk
                self.offset_ = 0
                self.position_ += remaining_bytes_in_chunk
                self.current_chunk = next(self.chunks)
            else:
                self.offset_ += __offset
                self.position_ += __offset
                __offset = 0
        return self.position_
