import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List

from dateutil.relativedelta import relativedelta
from persisty.errors import PersistyError
from persisty.store_meta import StoreMeta
from servey.security.authorization import Authorization
from servey.security.authorizer.authorizer_abc import AuthorizerABC
from servey.security.authorizer.authorizer_factory_abc import get_default_authorizer

from persisty_data.data_store_abc import DataStoreABC
from persisty_data.data_store_factory_abc import DataStoreFactoryABC
from persisty_data.form_field import FormField
from persisty_data.s3_client import get_s3_client
from persisty_data.upload_form import UploadForm


@dataclass
class S3DataStoreFactory(DataStoreFactoryABC):
    factory: DataStoreFactoryABC
    bucket_name: str
    authorizer: Optional[AuthorizerABC] = field(default_factory=get_default_authorizer)
    upload_expire_in: int = 3600
    download_expire_in: int = 3600

    def get_meta(self) -> StoreMeta:
        return self.factory.get_meta()

    def get_upload_form(
        self, key: str, authorization: Optional[Authorization]
    ) -> UploadForm:
        data_store = self.create(authorization)
        meta = data_store.get_meta()
        self._check_store_access(data_store, key, meta)
        conditions = self._build_conditions(meta)
        kwargs = {
            "Bucket": self.bucket_name,
            "Key":  key,
            "ExpiresIn": self.upload_expire_in,
        }
        if conditions:
            kwargs["Conditions"] = conditions

        s3_client = get_s3_client()
        response = s3_client.generate_presigned_post(**kwargs)
        return UploadForm(
            url=response["url"],
            expire_at=(datetime.now() + relativedelta(seconds=self.upload_expire_in)),
            pre_populated_fields=[
                FormField(k, v) for k, v in response["fields"].items()
            ],
        )

    def _check_store_access(self, data_store: DataStoreABC, key: str, meta: StoreMeta):
        store_access = meta.store_access
        if not (store_access.creatable and store_access.updatable):
            item = data_store.read(key)
            if item:
                if not store_access.updatable:
                    raise PersistyError("unavailable_operation")
                if not store_access.creatable:
                    raise PersistyError("unavailable_operation")

    def _build_conditions(self, meta: StoreMeta):
        conditions = []
        self._append_size_condition(meta, conditions)
        self._append_content_type_conditions(meta, conditions)
        return conditions

    def _append_size_condition(self, meta: StoreMeta, conditions: List):
        size_attr = next(a for a in meta.attrs if a.name == "size")
        schema = size_attr.schema.schema
        minimum = schema.get("minimum") or 0
        maximum = schema.get("maximum")
        if maximum:
            conditions.append(["content-length-range", minimum, maximum])

    def _append_content_type_conditions(self, meta: StoreMeta, conditions: List):
        content_type_attr = next(a for a in meta.attrs if a.name == "size")
        schema = content_type_attr.schema.schema
        enum = schema.get("enum")
        if enum:
            condition = ["content-type"]
            condition.extend(enum)
            conditions.append(condition)

    def get_download_url(self, key: str, authorization: Optional[Authorization]) -> str:
        data_store = self.create(authorization)
        item = data_store.read(key)
        download_url = item.data_url
        return download_url

    def create(self, authorization: Optional[Authorization]) -> Optional[DataStoreABC]:
        return self.factory.create(authorization)


def s3_data_store_factory(factory: DataStoreFactoryABC) -> DataStoreFactoryABC:
    return S3DataStoreFactory(
        factory=factory,
        bucket_name=os.environ.get(f"PERSISTY_DATA_S3_BUCKET_{factory.get_meta().name.upper()}")
    )
