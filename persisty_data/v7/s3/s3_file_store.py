import hashlib
import os
import shutil
from io import IOBase
from pathlib import Path
from typing import Optional
from uuid import UUID

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr

from persisty_data.v7.directory.directory_file_handle_writer import DirectoryFileHandleWriter
from persisty_data.v7.file_handle import FileHandle
from persisty_data.v7.persisty.persisty_file_handle import PersistyFileHandle
from persisty_data.v7.persisty.persisty_file_store_abc import PersistyFileStoreABC

COPY_BUFFER_SIZE = 1024 * 1024
zzzzz zzzzzzz

class S3FileStore(PersistyFileStoreABC):
    store_dir: Path
    upload_dir: Path

    def content_write(
        self,
        file_name: Optional[str],
        content_type: Optional[str] = None,
    ) -> IOBase:
        try:
            writer = open(_key_to_path(self.store_dir, file_name), "wb")
            writer = DirectoryFileHandleWriter(
                writer=writer,
                file_name=file_name,
                content_type=content_type
            )
            return writer
        except FileNotFoundError:
            pass

    def upload_write(
        self,
        part_id: UUID,
    ) -> Optional[IOBase]:
        upload_part = self.upload_part_store.read(str(part_id))
        if not upload_part:
            return
        try:
            file_name = _key_to_path(self.upload_dir, str(upload_part.upload_id))
            file_name.mkdir(parents=True, exist_ok=True)
            file_name = f"{upload_part.upload_id}/{part_id}"
            writer = open(_key_to_path(self.upload_dir, file_name), "wb")
            # noinspection PyTypeChecker
            return writer
        except FileNotFoundError:
            pass

    def content_read(self, file_name: str) -> Optional[IOBase]:
        file_handle = self.file_handle_store.read(self._to_key(file_name))
        if file_handle:
            # noinspection PyTypeChecker
            return open(_key_to_path(self.store_dir, file_name), "rb")

    def file_delete(self, file_name: str) -> bool:
        key = self._to_key(file_name)
        file_handle = self.file_handle_store.read(key)
        if not file_handle:
            return False
        # noinspection PyProtectedMember
        result = self.file_handle_store._delete(key, file_handle)
        if result:
            os.remove(_key_to_path(self.store_dir, file_name))
        return result

    def upload_finish(self, upload_id: UUID) -> Optional[FileHandle]:
        upload_handle = self.upload_handle_store.read(str(upload_id))
        if not upload_handle or upload_handle.store_name != self.meta.name:
            return
        file_handle_id = f"{self.meta.name}/{upload_handle.store_name}"
        file_handle = self.file_handle_store.read(str(upload_id))
        md5 = hashlib.md5()
        size_in_bytes = 0

        upload_parts = list(self.upload_part_store.search_all(
            AttrFilter("upload_id", AttrFilterOp.eq, upload_id),
            SearchOrder((SearchOrderAttr("part_number"),))
        ))
        with open(_key_to_path(self.store_dir, upload_handle.file_name), 'wb') as writer:
            for upload_part in upload_parts:
                file_name = _key_to_path(self.upload_dir, f"{upload_id}/{upload_part.id}")
                with open(file_name, 'rb') as reader:
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

    def upload_delete(self, upload_id: UUID) -> bool:
        key = str(upload_id)
        result = self.upload_handle_store.delete(key)
        if result:
            self.upload_part_store.delete_all(
                AttrFilter("upload_id", AttrFilterOp.eq, upload_id)
            )
            upload_dir = _key_to_path(self.upload_dir, str(upload_id))
            shutil.rmtree(upload_dir)
        return result


def _key_to_path(directory: Path, key: str):
    path = Path(directory, key)
    assert os.path.normpath(path) == str(path)  # Prevent ../ shenanigans
    return path
