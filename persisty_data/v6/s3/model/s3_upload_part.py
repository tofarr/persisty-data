import os
import tempfile
from typing import AsyncIterator

from persisty.attr.attr import Attr

from persisty_data.v6.model.store_model_abc import StoreModelABC
from persisty_data.v6.model.upload_part import UploadPart
from persisty_data.v6.s3.s3_client import get_s3_client
from persisty_data.v6.s3.generator.s3_copy_item_key_from_upload_generator import S3CopyItemKeyFromUploadGenerator
from persisty_data.v6.s3.generator.s3_signed_upload_url_generator import S3SignedUploadUrlGenerator


class S3UploadPart(UploadPart, StoreModelABC):
    item_key: str = Attr(creatable=False, create_generator=S3CopyItemKeyFromUploadGenerator())
    upload_url: str = Attr(
        creatable=False,
        create_generator=S3SignedUploadUrlGenerator(),
    )

    async def save_content(self, content: AsyncIterator[bytes]):
        """ Most uploads should be directly to S3 using the upload url and not go through this code """
        file = tempfile.SpooledTemporaryFile(mode="rwb")
        async for data in content:
            file.write(data)
        file.seek(0)
        get_s3_client().upload_part(
            Body=file,
            Bucket=self.get_bucket_name(),
            Key=self.item_key,
            UploadId=self.upload_id,
            PartNumber=self.part_number
        )

    @staticmethod
    def get_bucket_name():
        return os.environ['PERSISTY_DATA_S3_BUCKET_NAME']
