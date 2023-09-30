import os
from dataclasses import dataclass
from io import RawIOBase
from typing import Iterator, Optional

from persisty.util import UNDEFINED

from persisty_data.v6.chunk.chunk import Chunk


@dataclass
class ChunkReader(RawIOBase):
    chunks: Iterator[Chunk]
    current_chunk: Optional[Chunk] = UNDEFINED
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

    def seek(self, __offset: int, __whence: int = os.SEEK_CUR) -> int:
        to_skip = __offset
        if __offset < 0 or __whence != os.SEEK_CUR:
            raise NotImplementedError()
        while __offset:
            if self.current_chunk is UNDEFINED or (len(self.current_chunk.data) - self.offset_) < __offset:
                __offset -= len(self.current_chunk.data) - self.offset_
                self.current_chunk = next(self.chunks, None)
                self.offset_ = 0
            else:
                self.offset_ += __offset
                __offset = 0
        self.position_ += to_skip - __offset
        return self.position_
