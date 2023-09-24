import dataclasses
from enum import Enum
from pathlib import Path
from typing import Optional, Union
from urllib.request import urlopen

from persisty.attr.attr import Attr
from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.attr.attr_type import AttrType
from persisty.errors import PersistyError
from persisty.impl.dynamodb.partition_sort_index import PartitionSortIndex
from persisty.index.attr_index import AttrIndex
from persisty.security.store_security import INTERNAL_ONLY
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC
from persisty.store_meta import T, StoreMeta, get_meta
from persisty.stored import stored
from schemey import schema_from_type

from persisty_data.v4.chunk.chunk import Chunk
from persisty_data.v4.chunk.chunk_loader import ChunkLoader
from persisty_data.v4.chunk.chunked_file_handle import ChunkedFileHandle
from persisty_data.v4.file_handle import FileHandle
from persisty_data.v4.file_handle_status import FileHandleStatus


@dataclasses.dataclass
class ChunkedFileStore(WrapperStoreABC[FileHandle]):
    file_handle_store: StoreABC[ChunkedFileHandle]
    chunk_store: StoreABC[Chunk]
    url_pattern: str
    chunk_size: int = 1024 * 256
    max_file_size: Optional[int] = None
    meta: Optional[StoreMeta] = None

    def __post_init__(self):
        if not self.meta:
            meta = self.file_handle_store.get_meta()
            self.meta = dataclasses.replace(
                meta,
                name=meta.name.replace("_file_", ""),
                attrs=tuple(_wrap_attr(a) for a in meta.attrs),
            )

    def get_store(self):
        return self.file_handle_store

    def get_meta(self) -> StoreMeta:
        return self.meta

    def create(self, item: FileHandle) -> Optional[FileHandle]:
        file_handle_store = self.file_handle_store
        create_class = file_handle_store.get_meta().get_create_dataclass()
        create_item: ChunkedFileHandle = create_class(
            key=item.key,
            status=FileHandleStatus.PROCESSSING,
            content_type=item.content_type or None,
        )
        new_item = file_handle_store.create(create_item)
        self._write_chunks(new_item.key, item.handle)
        return new_item

    def _update(
        self,
        key: str,
        item: T,
        updates: T,
    ) -> Optional[FileHandle]:
        file_handle_store = self.file_handle_store
        update_class = file_handle_store.get_meta().get_create_dataclass()
        update_item: ChunkedFileHandle = update_class(
            key=item.key,
            status=FileHandleStatus.PROCESSSING,
            content_type=item.content_type or None,
        )
        updated_item = file_handle_store._update(key, item, update_item)
        self._write_chunks(updated_item.key, updates.handle)
        return updated_item

    def _delete(self, key: str, item: ChunkedFileHandle) -> bool:
        file_handle_store = self.file_handle_store
        result = file_handle_store._delete(key, item)
        if result:
            self._delete_chunks(key)
        return result

    def _delete_chunks(self, key: str, exclude_stream_id: Optional[str] = None):
        search_filter = AttrFilter("item_key", AttrFilterOp.eq, key)
        if exclude_stream_id:
            search_filter &= AttrFilter("stream_id", AttrFilterOp.ne, exclude_stream_id)
        chunks = self.chunk_store.search_all(search_filter)
        chunk_key_config = self.chunk_store.get_meta().key_config
        for chunk in chunks:
            chunk_key = chunk_key_config.to_key_str(chunk)
            self.chunk_store._delete(chunk_key, chunk)

    def _open_for_read(self, handle: Union[str, Path]):
        if isinstance(handle, str):
            if not handle.startswith("http://") and not handle.startswith("https://"):
                raise PersistyError("invalid_handle")
            return urlopen(handle)
        if isinstance(handle, Path):
            return open(handle)
        from starlette.datastructures import UploadFile

        if isinstance(handle, UploadFile):
            return handle

    def _write_chunks(self, key: str, handle: Union[str, Path]):
        if isinstance(handle, str):
            # noinspection HttpUrlsUsage
            if not handle.startswith("http://") and not handle.startswith("https://"):
                raise PersistyError("invalid_handle")

            return urlopen(handle)
        chunk_loader = ChunkLoader(
            self.file_handle_store,
            self.chunk_store,
            self.chunk_size,
            self.max_file_size,
        )
        if isinstance(handle, Path):
            # noinspection PyTypeChecker
            chunk_loader.copy_to_store(key, lambda: open(handle, "rb"))

        from starlette.datastructures import UploadFile

        if isinstance(handle, UploadFile):
            # noinspection PyTypeChecker
            chunk_loader.copy_to_store(key, lambda: handle)


def _wrap_attr(attr: Attr):
    if attr.name in ("status", "size_in_bytes", "etag"):
        attr = dataclasses.replace(attr, creatable=False, updatable=False)
    return attr


def chunked_file_store(
    name: str,
    permitted_content_types: Optional[Enum] = None,
    max_file_size: int = 1024 * 1024 * 100,
):
    chunk_store = get_meta(
        stored(
            type(f"{name}_chunk", (), Chunk.__dict__),
            indexes=(
                AttrIndex("stream_id"),
                PartitionSortIndex("stream_id", "part_number"),
            ),
            store_security=INTERNAL_ONLY,
        )
    ).create_store()
    file_handle_store_props = {**ChunkedFileHandle.__dict__}
    if permitted_content_types:
        file_handle_store_props["content_type"] = Attr(
            "content_type",
            AttrType.STR,
            schema_from_type(permitted_content_types.__class__),
        )
    file_handle_store = get_meta(
        stored(
            type(f"{name}_file_handle", (), file_handle_store_props),
            indexes=(
                AttrIndex("stream_id"),
                PartitionSortIndex("stream_id", "part_number"),
            ),
            store_security=INTERNAL_ONLY,
        )
    ).create_store()
    return ChunkedFileStore(
        file_handle_store=file_handle_store,
        chunk_store=chunk_store,
        url_pattern="/" + name + "/{key}",
        max_file_size=max_file_size,
    )
