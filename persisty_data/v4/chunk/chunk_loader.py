import hashlib
from dataclasses import dataclass
from io import IOBase
from typing import Callable
from uuid import uuid4

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.errors import PersistyError
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr
from persisty.store.store_abc import StoreABC

from persisty_data.chunk import Chunk
from persisty_data.v4.chunk.chunked_file_handle import ChunkedFileHandle
from persisty_data.v4.file_handle_status import FileHandleStatus
from persisty_data.v4.loader_abc import LoaderABC


@dataclass
class ChunkLoader(LoaderABC):
    file_handle_store: StoreABC[ChunkedFileHandle]
    chunk_store: StoreABC[Chunk]
    chunk_size: int
    max_file_size: int

    def load_from_store(self, source_key: str, destination: IOBase):
        pass

    def copy_from_store(self, source_key: str, destination: IOBase):
        file_handle = self.file_handle_store.read(source_key)
        if not file_handle:
            raise PersistyError("not_found")
        if not file_handle.status == FileHandleStatus.READY:
            raise PersistyError("not_ready")
        search_filter = AttrFilter("stream_id", AttrFilterOp.eq, file_handle.stream_id)
        search_order = SearchOrder((SearchOrderAttr("part_number"),))
        chunks = self.chunk_store.search_all(search_filter, search_order)
        for chunk in chunks:
            destination.write(chunk.data)

    def copy_to_store(self, source: Callable[[], IOBase], destination_key: str):
        file_handle_update_class = (
            self.file_handle_store.get_meta().get_update_dataclass()
        )
        # noinspection PyBroadException
        try:
            stream_id = str(uuid4())
            part_number = 0
            etag = hashlib.md5()
            size_in_bytes = 0
            chunk_create_class = self.chunk_store.get_meta().get_create_dataclass()
            with source() as reader:
                while True:
                    data = reader.read(self.chunk_size)
                    if not data:
                        file_handle = file_handle_update_class(
                            key=destination_key,
                            status=FileHandleStatus.READY,
                            stream_id=stream_id,
                            size_in_bytes=size_in_bytes,
                            etag=etag.hexdigest(),
                        )
                        self.file_handle_store.update(file_handle)
                        return
                    etag.update(data)
                    size_in_bytes += len(data)
                    if size_in_bytes > self.max_file_size:
                        raise PersistyError("max_size_exceeded")
                    chunk = chunk_create_class(
                        item_key=destination_key,
                        stream_id=stream_id,
                        part_number=part_number,
                        data=data,
                    )
                    self.chunk_store.create(chunk)
                    part_number += 1
        except Exception as e:
            file_handle = file_handle_update_class(
                key=destination_key, stream_id=None, status=FileHandleStatus.ERROR
            )
            self.file_handle_store.update(file_handle)
            raise e
