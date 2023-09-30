from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.util import UNDEFINED

from persisty_data.v6.s3.s3_client import get_s3_client


class S3UploadIdGenerator(AttrValueGeneratorABC):

    def transform(self, value, item):
        if value is UNDEFINED:
            response = get_s3_client().create_multipart_upload(
                Bucket=item.get_bucket_name(),
                Key=item.item_key
            )
            value = response["UploadId"]
        return value
