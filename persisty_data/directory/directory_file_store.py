import hashlib
import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from io import IOBase
from pathlib import Path
from typing import Optional
from uuid import UUID

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.batch_edit import BatchEdit
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr

from persisty_data.directory.directory_file_handle_writer import (
    DirectoryFileHandleWriter,
)
from persisty_data.file_handle import FileHandle
from persisty_data.persisty.persisty_file_handle import PersistyFileHandle
from persisty_data.persisty.persisty_file_store_abc import PersistyFileStoreABC

COPY_BUFFER_SIZE = 1024 * 1024


@dataclass
class DirectoryFileStore(PersistyFileStoreABC):
    store_dir: Path = None
    upload_dir: Path = None

    def __post_init__(self):
        if not self.store_dir:
            self.store_dir = Path(self.meta.name, "store")
        if not self.upload_dir:
            self.store_dir = Path(self.meta.name, "upload")

    def content_write(
        self,
        file_name: Optional[str],
        content_type: Optional[str] = None,
    ) -> IOBase:
        try:
            writer = open(key_to_path(self.store_dir, file_name), "wb")
            writer = DirectoryFileHandleWriter(
                writer=writer, file_name=file_name, content_type=content_type
            )
            return writer
        except FileNotFoundError:
            pass

    def upload_write(
        self,
        part_id: str,
    ) -> Optional[IOBase]:
        upload_part = self.upload_part_store.read(part_id)
        if not upload_part:
            return
        try:
            file_name = key_to_path(self.upload_dir, str(upload_part.upload_id))
            file_name.mkdir(parents=True, exist_ok=True)
            file_name = f"{upload_part.upload_id}/{part_id}"
            writer = open(key_to_path(self.upload_dir, file_name), "wb")
            # noinspection PyTypeChecker
            return writer
        except FileNotFoundError:
            pass

    def content_read(self, file_name: str) -> Optional[IOBase]:
        file_handle = self.file_handle_store.read(self._to_key(file_name))
        if file_handle:
            # noinspection PyTypeChecker
            return open(key_to_path(self.store_dir, file_name), "rb")

    def file_delete(self, file_name: str) -> bool:
        key = self._to_key(file_name)
        file_handle = self.file_handle_store.read(key)
        if not file_handle:
            return False
        # noinspection PyProtectedMember
        result = self.file_handle_store._delete(key, file_handle)
        if result:
            os.remove(key_to_path(self.store_dir, file_name))
        return result

    def upload_finish(self, upload_id: UUID) -> Optional[FileHandle]:
        upload_handle = self.upload_handle_store.read(str(upload_id))
        if not upload_handle or upload_handle.store_name != self.meta.name:
            return
        file_handle_id = f"{self.meta.name}/{upload_handle.store_name}"
        file_handle = self.file_handle_store.read(file_handle_id)
        md5 = hashlib.md5()
        size_in_bytes = 0

        upload_parts = list(
            self.upload_part_store.search_all(
                AttrFilter("upload_id", AttrFilterOp.eq, upload_id),
                SearchOrder((SearchOrderAttr("part_number"),)),
            )
        )
        with open(key_to_path(self.store_dir, upload_handle.file_name), "wb") as writer:
            for upload_part in upload_parts:
                file_name = key_to_path(
                    self.upload_dir, f"{upload_id}/{upload_part.id}"
                )
                with open(file_name, "rb") as reader:
                    buffer = reader.read(COPY_BUFFER_SIZE)
                    writer.write(buffer)
                    md5.update(buffer)
                    size_in_bytes += len(buffer)
                os.remove(file_name)

        new_file_handle = PersistyFileHandle(
            id=file_handle_id,
            file_name=upload_handle.file_name,
            upload_id=upload_handle.id,
            content_type=upload_handle.content_type,
            etag=md5.hexdigest(),
            size_in_bytes=size_in_bytes,
        )
        self.upload_handle_store.delete(str(upload_id))
        if file_handle:
            # noinspection PyProtectedMember
            file_handle = self.file_handle_store._update(
                file_handle_id, file_handle, new_file_handle
            )
        else:
            file_handle = self.file_handle_store.create(new_file_handle)
        return self._to_file_handle(file_handle)

    def upload_delete(self, upload_id: str) -> bool:
        result = self.upload_handle_store.delete(upload_id)
        if result:
            self.upload_part_store.delete_all(
                AttrFilter("upload_id", AttrFilterOp.eq, upload_id)
            )
            upload_dir = key_to_path(self.upload_dir, upload_id)
            shutil.rmtree(upload_dir)
        return result

    def directory_sync(self):
        self.file_handle_store.edit_all(self.directory_sync_iterator())

    def directory_sync_iterator(self):
        paths = self.store_dir.rglob(str(self.store_dir))
        paths = {path for path in paths if path.is_file()}
        for file_handle in self.file_handle_store.search_all():
            file_path = key_to_path(self.store_dir, file_handle.file_name)
            if file_path in paths:
                stats = os.stat(file_path)
                etag = file_hash(file_path)
                updated_at = datetime.fromtimestamp(stats.st_mtime)
                if file_handle.etag != etag or file_handle.updated_at != updated_at:
                    file_handle.etag = etag
                    file_handle.size_in_bytes = stats.st_size
                    file_handle.updated_at = updated_at
                    yield BatchEdit(update_item=file_handle)
            else:
                yield BatchEdit(
                    delete_key=str(file_path)[: len(str(self.store_dir)) + 1]
                )
        for path in paths:
            file_name = str(path)[: len(str(self.store_dir)) + 1]
            etag = file_hash(path)
            stats = os.stat(path)
            updated_at = datetime.fromtimestamp(stats.st_mtime)
            yield BatchEdit(
                create_item=PersistyFileHandle(
                    id=f"{self.meta.name}/{file_name}",
                    store_name=self.meta.name,
                    file_name=file_name,
                    etag=etag,
                    size_in_bytes=stats.st_size,
                    created_at=updated_at,
                    updated_at=datetime.fromtimestamp(stats.st_mtime),
                )
            )


def key_to_path(directory: Path, key: str):
    path = Path(directory, key)
    assert os.path.normpath(path) == str(path)  # Prevent ../ shenanigans
    return path


def file_hash(path: Path) -> str:
    hash_ = hashlib.md5()
    with open(path, "rb") as reader:
        while True:
            buffer = reader.read(COPY_BUFFER_SIZE)
            if not buffer:
                return hash_.hexdigest()
            hash_.update(buffer)
