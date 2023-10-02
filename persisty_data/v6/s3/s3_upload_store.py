from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC

from persisty_data.v6.s3.model.s3_upload import S3Upload
from persisty_data.v6.s3.s3_client import get_s3_client


class S3UploadStore(WrapperStoreABC):
    store: StoreABC[S3Upload]

    def create(self, item: S3Upload) -> S3Upload:
        s3_client = get_s3_client()
        s3_client.create_multipart_upload(
            Bucket=self.get_bucket_name(), Key=self.item_key, UploadId=self.id
        )
        return self.get_store().create(item)
