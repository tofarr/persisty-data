from dataclasses import dataclass
from typing import Dict, Generic, List, Optional, Type, TypeVar

from marshy.factory.optional_marshaller_factory import get_optional_type
from marshy.types import ExternalItemType
from persisty.attr.generator.default_value_generator import DefaultValueGenerator
from schemey import schema_from_type
from servey.security.authorization import Authorization

from persisty.attr.attr import Attr, DEFAULT_PERMITTED_FILTER_OPS
from persisty.attr.attr_type import AttrType
from persisty.errors import PersistyError
from persisty.link.link_abc import LinkABC

from persisty_data.file_store_abc import FileStoreABC
from persisty_data.finder.file_store_finder_abc import find_file_stores

T = TypeVar("T")


class HasUrlCallable(Generic[T]):
    def __init__(self, key: str, file_store: FileStoreABC):
        self.key = key
        self.file_store = file_store

    def __call__(self, authorization: Optional[Authorization] = None) -> Optional[T]:
        file_store = self.file_store
        store_security = file_store.get_meta().store_security
        secured_file_store = store_security.get_secured(file_store, authorization)
        file_handle = secured_file_store.file_read(self.key)
        return file_handle.download_url


@dataclass
class HasUrl(LinkABC):
    name: Optional[str] = None
    file_store_name: Optional[str] = None
    key_attr_name: Optional[str] = None
    optional: Optional[bool] = None

    def __set_name__(self, owner, name):
        self.name = name
        if self.file_store_name is None and name.endswith("_url"):
            self.file_store_name = name[:-4]
        if self.key_attr_name is None:
            self.key_attr_name = f"{self.file_store_name}_key"
        if self.optional is None:
            url_type = owner.__dict__["__annotations__"].get(name)
            self.optional = bool(get_optional_type(url_type))

    def __get__(self, obj, obj_type) -> HasUrlCallable[T]:
        return HasUrlCallable(
            key=getattr(obj, self.key_attr_name),
            file_store=self.get_linked_file_store(),
        )

    async def batch_call(
        self, keys: List, authorization: Optional[Authorization] = None
    ) -> List[Optional[T]]:
        if not keys:
            return []
        result = list(
            self.get_linked_file_store().get_all_download_urls(
                iter(keys), authorization
            )
        )
        return result

    def arg_extractor(self, obj):
        return [getattr(obj, self.key_attr_name)]

    def get_name(self) -> str:
        return self.name

    def get_linked_type(self, forward_ref_ns: str) -> Type[Optional[str]]:
        return Optional[str]

    def get_linked_file_store(self):
        file_store = getattr(self, "_file_store", None)
        if not file_store:
            file_store = next(
                (
                    f
                    for f in find_file_stores()
                    if f.get_meta().name == self.file_store_name
                ),
                None,
            )
            if file_store is None:
                raise PersistyError(f"unknown_file_store:{self.file_store_name}")
            setattr(self, "_file_store", file_store)
        return file_store

    def update_attrs(self, attrs_by_name: Dict[str, Attr]):
        if self.key_attr_name in attrs_by_name:
            return
        type_ = Optional[str] if self.optional else str
        schema = schema_from_type(type_)
        create_generator = DefaultValueGenerator(None) if self.optional else None
        attrs_by_name[self.key_attr_name] = Attr(
            self.key_attr_name,
            AttrType.STR,
            schema,
            sortable=False,
            create_generator=create_generator,
            permitted_filter_ops=DEFAULT_PERMITTED_FILTER_OPS,
        )

    def update_json_schema(self, json_schema: ExternalItemType):
        key_attr_schema = json_schema.get("properties").get(self.key_attr_name)
        key_attr_schema["persistyHasUrl"] = {
            "fileStoreName": self.file_store_name
        }
