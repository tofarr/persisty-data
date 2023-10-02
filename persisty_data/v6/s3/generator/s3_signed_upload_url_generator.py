from dataclasses import dataclass

from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.util import UNDEFINED

from persisty_data.v6.s3.s3_client import get_s3_client


class S3SignedUploadUrlGenerator(AttrValueGeneratorABC):
    def transform(self, value, item):
        if value is UNDEFINED:
            value = get_s3_client().generate_presigned_url(
                ClientMethod="upload_part",
                Params={
                    "Bucket": item.bucket_name,
                    "Key": item.item_key,
                    "UploadId": item.upload_id,
                    "PartNumber": item.part_number,
                },
            )
        return value
