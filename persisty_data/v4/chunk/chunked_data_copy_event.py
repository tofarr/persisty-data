from dataclasses import dataclass
from urllib.request import urlopen

from persisty_data.v4.chunk.chunked_file_handle import ChunkedFileHandle


@dataclass
class ChunkedDataCopyEvent:
    chunk_store_name: str
    file_handle_store_name: str
    chunked_file_handle: ChunkedFileHandle
    chunk_size: int
    source_url: str

    def copy_data_to_chunks(self):
        from persisty.finder.stored_finder_abc import find_stored_by_name
        from persisty_data.v4.chunk.chunked_file_store import copy_data_to_chunks

        chunk_store = find_stored_by_name(self.chunk_store_name)
        file_handle_store = find_stored_by_name(self.file_handle_store_name)
        copy_data_to_chunks(
            source=self.open,
            item=self.chunked_file_handle,
            chunk_size=self.chunk_size,
            chunk_store=chunk_store.create_store(),
            file_handle_store=file_handle_store.create_store(),
        )

    def open(self):
        return urlopen(self.source_url)
