from dataclasses import field, dataclass
from io import IOBase
from tempfile import SpooledTemporaryFile
from types import TracebackType
from typing import Any, Optional, Union

from persisty.store.store_abc import StoreABC
from persisty.store_meta import get_meta

from persisty_data.persisty_store.persisty_file_handle import PersistyFileHandle
from persisty_data.s3.s3_client import get_s3_client


@dataclass
class S3ContentWriter(IOBase):
    """
    Spools content to local temp file before uploading to S3. Ideally, uploads bypass the app server using
    upload urls and do not call this
    """

    store_name: str
    bucket_name: str
    file_name: str
    content_type: Optional[str]
    file: Any = field(default_factory=SpooledTemporaryFile)
    file_handle_store: StoreABC[PersistyFileHandle] = field(
        default_factory=get_meta(PersistyFileHandle).create_store
    )
    size_in_bytes: int = 0

    def __enter__(self):
        self.file.__enter__()

    def write(self, __b) -> Union[int, None]:
        result = self.file.write(__b)
        if result:
            self.size_in_bytes += result
        return result

    def __exit__(
        self,
        exc_type: Union[type[BaseException], None],
        exc_val: Union[BaseException, None],
        exc_tb: Union[TracebackType, None],
    ) -> None:
        self.file.seek(0)
        response = get_s3_client().put_object(
            Body=self.file, Key=self.file_name, Bucket=self.bucket_name
        )
        key = f"{self.store_name}/{self.file_name}"
        file_handle = self.file_handle_store.read(key)
        updates = PersistyFileHandle(
            id=key,
            file_name=self.file_name,
            content_type=self.content_type,
            etag=response["ETag"],
            size_in_bytes=self.size_in_bytes,
        )
        if file_handle:
            # noinspection PyProtectedMember
            self.file_handle_store._update(key, file_handle, updates)
        else:
            self.file_handle_store.create(file_handle)

        self.file.__exit__(exc_type, exc_val, exc_tb)
