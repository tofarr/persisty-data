import os
from pathlib import Path
from typing import BinaryIO

from persisty.store_meta import get_meta

from persisty_data.v6.model.file_handle import FileHandle
from persisty_data.v6.s3.s3_client import get_s3_client


class S3FileHandle(FileHandle):
    def get_reader(self) -> BinaryIO:
        s3_client = get_s3_client()
        response = s3_client.get_object(
            Bucket=self.get_bucket_name(),
            Key=self.key,
        )
        streaming_body = response["Body"]
        return streaming_body

    def get_bucket_name(self):
        return os.environ["PERSISTY_DATA_S3_BUCKET_NAME"]

    def get_download_url(self):
        download_url_pattern = os.environ["PERSISTY_DATA_S3_DOWNLOAD_URL_PATTERN"]
        download_url = download_url_pattern.format(key=self.item_key)
        return download_url
