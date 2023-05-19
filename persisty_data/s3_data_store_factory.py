from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from dateutil.relativedelta import relativedelta
from persisty.errors import PersistyError
from persisty.store.store_abc import StoreABC
from persisty.store_meta import T, StoreMeta
from servey.security.authorization import Authorization
from servey.security.authorizer.authorizer_abc import AuthorizerABC

from persisty_data.data_store_factory_abc import DataStoreFactoryABC
from persisty_data.form_field import FormField
from persisty_data.s3_client import get_s3_client
from persisty_data.s3_data_store import S3DataStore
from persisty_data.upload_form import UploadForm


@dataclass
class S3DataStoreFactory(DataStoreFactoryABC):
    store: S3DataStore
    bucket_name: str
    authorizer: Optional[AuthorizerABC]
    upload_expire_in: int = 3600
    download_expire_in: int = 3600

    def get_meta(self) -> StoreMeta:
        return self.store.store_meta

    def get_upload_form(
        self, key: str, authorization: Optional[Authorization]
    ) -> UploadForm:
        data_store = self.create(authorization)
        store_access = data_store.get_meta().store_access
        if not (store_access.creatable and store_access.updatable):
            item = data_store.read(key)
            if item:
                if not store_access.updatable:
                    raise PersistyError("unavailable_operation")
                else:
                    if not store_access.creatable:
                        raise PersistyError("unavailable_operation")
        s3_client = get_s3_client()
        response = s3_client.generate_presigned_post(
            Bucket=self.bucket_name, Key=key, ExpiresIn=self.upload_expire_in
        )
        return UploadForm(
            url=response["url"],
            expire_at=(datetime.now() + relativedelta(seconds=self.upload_expire_in)),
            pre_populated_fields=[
                FormField(k, v) for k, v in response["fields"].items()
            ],
        )

    def get_download_url(self, key: str, authorization: Optional[Authorization]) -> str:
        data_store = self.create(authorization)
        item = data_store.read(key)
        download_url = item.data_url
        return download_url

    def create(self, authorization: Optional[Authorization]) -> Optional[StoreABC[T]]:
        return self.store
