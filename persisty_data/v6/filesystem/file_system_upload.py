import hashlib
from pathlib import Path
from typing import Optional

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr
from persisty.store_meta import get_meta
from servey.security.authorization import Authorization

from persisty_data.v6.filesystem.file_system_file_handle import FileSystemFileHandle, key_to_path, DATA_DIRECTORY
from persisty_data.v6.model.store_model_abc import StoreModelABC
from persisty_data.v6.model.upload import Upload


class FileSystemUpload(Upload, StoreModelABC):
    """
    This implementation relies on standard stores for sorting / filtering, since the file system cannot
    provide a full set of operations. A sync operation is provided to sync details from the file system
    to storage
    """

    def finish_upload(self, authorization: Optional[Authorization]) -> FileSystemFileHandle:
        file_handle_store_meta = self.get_file_handle_store_meta()
        upload_part_store_meta = self.get_upload_part_store_meta()
        search_filter = AttrFilter("upload_id", AttrFilterOp.eq, self.id)
        search_order = SearchOrder((SearchOrderAttr("part_number"),))
        upload_parts = upload_part_store_meta.search_all(search_filter, search_order)
        md5 = hashlib.md5()
        size_in_bytes = 0
        data_directory = Path(self.get_data_directory(), file_handle_store_meta.name)
        data_directory.mkdir(parents=True, exist_ok=True)
        destination = key_to_path(data_directory, self.item_key)
        with open(destination, 'wb') as writer:
            for upload_part in upload_parts:
                source = Path(self.get_data_directory(), upload_part_store_meta.name, str(upload_part.id))
                with open(source, 'rb') as reader:
                    while True:
                        data = reader.read(self.get_buffer_size())
                        if not data:
                            break
                        md5.update(data)
                        size_in_bytes += len(data)
                        writer.write(data)

        file_handle = file_handle_store_meta.get_create_dataclass()(
            key=self.item_key,
            content_type=self.content_type,
            size_in_bytes=size_in_bytes,
            etag=md5.hexdigest(),
            subject_id=authorization.subject_id if authorization else None,
            download_url="/data/"
            + file_handle_store_meta.name.replace("_", "-")
            + "/"
            + self.item_key,
            upload_id=str(self.id),
        )
        file_handle_store = file_handle_store_meta.create_store()
        file_handle = file_handle_store.create(file_handle)
        get_meta(self.__class__).create_store().delete(self.id)
        return file_handle

    def after_delete(self):
        search_filter = AttrFilter("upload_id", AttrFilterOp.eq, str(self.id))
        upload_part_store_meta = self.get_upload_part_store_meta()
        upload_part_store_meta.create_store().delete_all(search_filter)

    @classmethod
    def base_store_name(cls):
        result = get_meta(cls).name[:-7]
        return result

    @staticmethod
    def get_data_directory() -> str:
        return DATA_DIRECTORY

    @staticmethod
    def get_buffer_size() -> int:
        return 256 * 1024
