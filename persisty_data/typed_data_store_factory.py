import dataclasses
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Iterator, Set

from persisty.factory.store_factory_abc import ROUTE
from persisty.store.store_abc import StoreABC
from persisty.store_meta import T, StoreMeta
from schemey import schema_from_type
from servey.action.action import Action
from servey.security.authorization import Authorization

from persisty_data.data_store_factory_abc import DataStoreFactoryABC
from persisty_data.typed_data_store import TypedDataStore
from persisty_data.upload_form import UploadForm


@dataclass
class TypedDataStoreFactory(DataStoreFactoryABC):
    data_store_factory: DataStoreFactoryABC
    content_types: Set[str]
    meta: Optional[StoreMeta] = None

    def __post_init__(self):
        meta = self.meta
        if meta is None:
            meta = self.data_store_factory.get_meta()
            updated_attrs = []
            for attr in meta.attrs:
                if attr.name == 'content_type':
                    schema = schema_from_type(Enum("ContentTypes", list(self.content_types)))
                    attr = dataclasses.replace(attr, schema=schema)
                updated_attrs.append(attr)
            meta = dataclasses.replace(meta, attrs=tuple(updated_attrs))
            self.meta = meta

    def get_meta(self) -> StoreMeta:
        return self.meta

    def create(self, authorization: Optional[Authorization]) -> Optional[StoreABC[T]]:
        store = TypedDataStore(
            store=self.data_store_factory.create(authorization),
            content_types=self.content_types,
        )
        return store

    def get_upload_form(
        self, key: str, authorization: Optional[Authorization]
    ) -> UploadForm:
        form = self.data_store_factory.get_upload_form(key, authorization)
        form.content_types = self.content_types
        return form

    def get_download_url(self, key: str, authorization: Optional[Authorization]) -> str:
        return self.data_store_factory.get_download_url(key, authorization)

    def create_routes(self) -> Iterator[ROUTE]:
        return self.data_store_factory.create_routes()

    def create_actions(self) -> Iterator[Action]:
        return self.data_store_factory.create_actions()

    def get_json_schema(self):
        schema = self.data_store_factory.get_json_schema()
        schema["content_types"] = list(self.content_types)
        return schema
