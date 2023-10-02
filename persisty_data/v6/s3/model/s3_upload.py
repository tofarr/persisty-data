import os
from typing import Optional

from persisty.attr.attr import Attr
from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.store_meta import get_meta
from servey.security.authorization import Authorization

from persisty_data.v6.filesystem.file_system_file_handle import FileSystemFileHandle
from persisty_data.v6.model.store_model_abc import StoreModelABC
from persisty_data.v6.model.upload import Upload
from persisty_data.v6.s3.generator.s3_upload_id_generator import S3UploadIdGenerator
from persisty_data.v6.s3.s3_client import get_s3_client


class S3Upload(Upload, StoreModelABC):
    """
    This implementation relies on standard stores for sorting / filtering, since the file system cannot
    provide a full set of operations. A sync operation is provided to sync details from the file system
    to storage
    """

    id: str = Attr(creatable=False, create_generator=S3UploadIdGenerator())
    finished: bool = False

    def finish_upload(
        self, authorization: Optional[Authorization]
    ) -> FileSystemFileHandle:
        s3_client = get_s3_client()
        s3_client.complete_multipart_upload(
            Bucket=self.get_bucket_name(), Key=self.item_key, UploadId=self.id
        )
        upload_store_meta = self.get_upload_store_meta()
        upload_store = upload_store_meta.create_store()
        updates = upload_store_meta.get_update_dataclass()(id=self.id, finished=True)
        upload_store.update(updates)
        upload_store.delete(self.id)
        file_handle_store_meta = self.get_file_handle_store_meta()
        result = file_handle_store_meta.create_store().read(self.item_key)
        return result

    def after_delete(self):
        if not self.finished:
            search_filter = AttrFilter("upload_id", AttrFilterOp.eq, str(self.id))
            upload_part_store_meta = self.get_upload_part_store_meta()
            upload_part_store_meta.create_store().delete_all(search_filter)

    @classmethod
    def base_store_name(cls):
        result = get_meta(cls).name[:-7]
        return result

    @staticmethod
    def get_bucket_name():
        return os.environ["PERSISTY_DATA_S3_BUCKET_NAME"]

    """
    def get_download_url(self):
        download_url_pattern = os.environ['PERSISTY_DATA_S3_DOWNLOAD_URL_PATTERN']
        download_url = download_url_pattern.format(key=self.item_key)
        return download_url
    """
