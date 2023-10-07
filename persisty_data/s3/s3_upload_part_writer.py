from dataclasses import field, dataclass
from io import IOBase
from tempfile import SpooledTemporaryFile
from types import TracebackType
from typing import Any

from persisty_data.s3.s3_client import get_s3_client


@dataclass
class S3UploadPartWriter(IOBase):
    """
    Spools content to local temp file before uploading to S3. Ideally, uploads bypass the app server using
    upload urls and do not call this
    """

    bucket_name: str
    file_name: str
    upload_id: str
    part_number: int
    file: Any = field(default_factory=SpooledTemporaryFile)

    def __enter__(self):
        self.file.__enter__()

    def write(self, __b) -> int | None:
        return self.file.write(__b)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.file.seek(0)
        get_s3_client().upload_part(
            Bucket=self.bucket_name,
            Key=self.file_name,
            UploadId=self.upload_id,
            PartNumber=self.part_number,
            Body=self.file,
        )
        self.file.__exit__(exc_type, exc_val, exc_tb)
