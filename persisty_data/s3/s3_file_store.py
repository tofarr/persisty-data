from dataclasses import dataclass
from io import IOBase
from typing import Optional, Iterator

from persisty.attr.attr_filter import attr_eq
from persisty.batch_edit import BatchEdit

from persisty_data.file_handle import FileHandle
from persisty_data.persisty_store.persisty_file_handle import PersistyFileHandle
from persisty_data.persisty_store.persisty_file_store_abc import PersistyFileStoreABC
from persisty_data.persisty_store.persisty_upload_handle import PersistyUploadHandle
from persisty_data.persisty_store.persisty_upload_part import PersistyUploadPart
from persisty_data.s3.s3_client import get_s3_client
from persisty_data.s3.s3_content_writer import S3ContentWriter
from persisty_data.s3.s3_upload_part_writer import S3UploadPartWriter
from persisty_data.upload_part import UploadPart

COPY_BUFFER_SIZE = 1024 * 1024
_Route = "starlette.routing.Route"


@dataclass
class S3FileStore(PersistyFileStoreABC):
    bucket_name: str = None
    signed_download_urls: bool = False
    signed_upload_urls: bool = True

    def __post_init__(self):
        if not self.bucket_name:
            self.bucket_name = self.meta.name

    def get_routes(self) -> Iterator[_Route]:
        # No routes because we don't want uploads going through the app server - they should go directly to S3.
        return iter(tuple())

    def content_write(
        self,
        file_name: Optional[str],
        content_type: Optional[str] = None,
    ) -> IOBase:
        return S3ContentWriter(
            store_name=self.meta.name,
            bucket_name=self.bucket_name,
            file_name=file_name,
            file_handle_store=self.file_handle_store,
            content_type=content_type,
        )

    def upload_write(
        self,
        part_id: str,
    ) -> Optional[IOBase]:
        upload_part = self.upload_part_store.read(part_id)
        if not upload_part:
            return
        upload_handle = self.upload_handle_store.read(upload_part.upload_id)
        if not upload_handle:
            return
        return S3UploadPartWriter(
            bucket_name=self.bucket_name,
            file_name=upload_handle.file_name,
            upload_id=upload_part.upload_id,
            part_number=upload_part.part_number + 1,
        )

    def content_read(self, file_name: str) -> Optional[IOBase]:
        response = get_s3_client().get_object(Bucket=self.bucket_name, Key=file_name)
        result = response.get("Body")
        return result

    def _to_file_handle(
        self, file_handle: Optional[PersistyFileHandle]
    ) -> Optional[FileHandle]:
        result = super()._to_file_handle(file_handle)
        if result and self.signed_download_urls:
            result.download_url = get_s3_client().generate_presigned_url(
                ClientMethod="get_object",
                Params={"Bucket": self.bucket_name, "Key": file_handle.file_name},
            )
        return result

    def file_delete(self, file_name: str) -> bool:
        key = self._to_key(file_name)
        file_handle = self.file_handle_store.read(key)
        if not file_handle:
            return False
        # noinspection PyProtectedMember
        result = self.file_handle_store._delete(key, file_handle)
        if not result:
            return False
        response = get_s3_client().delete_object(Bucket=self.bucket_name, Key=file_name)
        result = response["DeleteMarker"]
        return result

    def upload_finish(self, upload_id: str) -> Optional[FileHandle]:
        upload_handle = self.upload_handle_store.read(upload_id)
        if not upload_handle or upload_handle.store_name != self.meta.name:
            return
        get_s3_client().complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=upload_handle.file_name,
            UploadId=upload_handle.id,
        )
        response = get_s3_client().head_object(
            Bucket=self.bucket_name, Key=upload_handle.file_name
        )

        file_handle_id = f"{self.meta.name}/{upload_handle.store_name}"
        file_handle = self.file_handle_store.read(file_handle_id)
        new_file_handle = PersistyFileHandle(
            id=file_handle_id,
            file_name=upload_handle.file_name,
            upload_id=upload_handle.id,
            content_type=upload_handle.content_type,
            etag=response["ETag"],
            size_in_bytes=response["ContentLength"],
            updated_at=response["LastModified"],
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
        upload_handle = self.upload_handle_store.read(upload_id)
        if not upload_handle:
            return False
        # noinspection PyProtectedMember
        result = self.upload_handle_store._delete(upload_id, upload_handle)
        if result:
            self.upload_part_store.delete_all(attr_eq("upload_id", upload_id))
            get_s3_client().abort_multipart_upload(
                Bucket=self.bucket_name,
                Key=upload_handle.file_name,
                UploadId=upload_id,
            )
        return result

    def _to_upload_part(
        self,
        upload_part: Optional[PersistyUploadPart],
        upload_handle: PersistyUploadHandle,
    ) -> Optional[UploadPart]:
        result = super()._to_upload_part(upload_part, upload_handle)
        if result and self.signed_upload_urls:
            result.upload_url = get_s3_client().generate_presigned_url(
                ClientMethod="upload_part",
                Params={
                    "Bucket": self.bucket_name,
                    "Key": upload_handle.file_name,
                    "UploadId": upload_handle.id,
                    "PartNumber": upload_part.part_number + 1,
                },
            )
        return result

    def bucket_sync(self):
        self.file_handle_store.edit_all(self.bucket_sync_iterator())

    def bucket_sync_iterator(self):
        bucket_objects = {obj.file_name: obj for obj in self.get_all_bucket_objects()}
        for file_handle in self.file_handle_store.search_all():
            bucket_file_handle = bucket_objects.pop(file_handle.file_name, None)
            if bucket_file_handle:
                if (
                    bucket_file_handle.etag != file_handle.etag
                    or bucket_file_handle.updated_at != file_handle.updated_at
                ):
                    yield BatchEdit(update_item=bucket_file_handle)
            else:
                yield BatchEdit(delete_key=file_handle.id)
        for bucket_object in bucket_objects.values():
            yield BatchEdit(create_item=bucket_object)

    def get_all_bucket_objects(self):
        s3_client = get_s3_client()
        kwargs = {"Bucket": self.bucket_name}
        while True:
            response = s3_client.list_objects(**kwargs)
            contents = response.get("Contents")
            if contents:
                for content in contents:
                    file_handle = PersistyFileHandle(
                        id=f"{self.meta.name}/{content['Key']}",
                        store_name=self.meta.name,
                        file_name=content["Key"],
                        etag=content["ETag"],
                        size_in_bytes=content["Size"],
                        created_at=content["LastModified"],
                        updated_at=content["LastModified"],
                    )
                    yield file_handle
            next_marker = response.get("NextMarker")
            if next_marker:
                kwargs["Marker"] = next_marker
            else:
                return
