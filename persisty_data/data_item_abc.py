import dataclasses
import io
from abc import abstractmethod, ABC
from datetime import datetime
from enum import Enum
from typing import Optional, FrozenSet

from marshy.marshaller.obj_marshaller import ObjMarshaller, attr_config
from marshy.marshaller_context import MarshallerContext
from schemey import schema_from_type
from schemey.schema import (
    str_schema,
    int_schema,
    datetime_schema,
    optional_schema,
    Schema,
)

from persisty.attr.attr import Attr
from persisty.attr.attr_filter_op import (
    STRING_FILTER_OPS,
    SORTABLE_FILTER_OPS,
    FILTER_OPS,
)
from persisty.attr.attr_type import AttrType
from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.store_meta import StoreMeta


class DataItemABC(ABC):
    @property
    @abstractmethod
    def key(self) -> str:
        """
        Get the key for this item
        """

    @property
    @abstractmethod
    def updated_at(self) -> Optional[datetime]:
        """
        Get the last modified date for this item. Returns none if the item was never updated
        """

    @property
    @abstractmethod
    def etag(self) -> Optional[str]:
        """
        Get the etag for this resource. Return none if the item has no content
        """

    @property
    @abstractmethod
    def content_type(self) -> Optional[str]:
        """
        Get the mime type for this item
        """

    @property
    @abstractmethod
    def size(self) -> Optional[int]:
        """
        Get the size in bytes of this item
        """

    @property
    @abstractmethod
    def data_url(self) -> Optional[str]:
        """
        Get the size in bytes of this item
        """

    @abstractmethod
    def get_data_reader(self) -> io.IOBase:
        """
        Get a reader for this item
        """

    # noinspection PyUnusedLocal
    @classmethod
    def __marshaller_factory__(cls, marshaller_context: MarshallerContext):
        from persisty_data.mem_data_item import MemDataItem

        marshaller = ObjMarshaller(
            MemDataItem,
            (
                attr_config(marshaller_context.get_marshaller(str), "key"),
                attr_config(marshaller_context.get_marshaller(Optional[int]), "size"),
                attr_config(
                    marshaller_context.get_marshaller(Optional[str]), "content_type"
                ),
                attr_config(
                    marshaller_context.get_marshaller(Optional[datetime]), "updated_at"
                ),
                attr_config(marshaller_context.get_marshaller(Optional[str]), "etag"),
            ),
        )
        return marshaller


DATA_ITEM_META = StoreMeta(
    name="data_item",
    attrs=(
        Attr(
            "key",
            AttrType.STR,
            str_schema(min_length=1, max_length=255),
            sortable=True,
            permitted_filter_ops=STRING_FILTER_OPS,
        ),
        Attr(
            "size",
            AttrType.INT,
            int_schema(minimum=0),
            sortable=True,
            permitted_filter_ops=SORTABLE_FILTER_OPS,
        ),
        Attr(
            "content_type",
            AttrType.STR,
            optional_schema(str_schema(max_length=255)),
            sortable=True,
            permitted_filter_ops=STRING_FILTER_OPS,
        ),
        Attr(
            "updated_at",
            AttrType.DATETIME,
            datetime_schema(),
            sortable=True,
            permitted_filter_ops=SORTABLE_FILTER_OPS,
        ),
        Attr(
            "etag",
            AttrType.STR,
            str_schema(max_length=255),
            sortable=False,
            permitted_filter_ops=FILTER_OPS,
        ),
        Attr(
            "data_url",
            AttrType.STR,
            optional_schema(str_schema()),
            sortable=False,
            permitted_filter_ops=tuple(),
        ),
    ),
    key_config=AttrKeyConfig("key"),
)


def data_item_meta(
    name: str, max_size: int, content_types: Optional[FrozenSet[str]] = None
):
    attrs = []
    for attr in DATA_ITEM_META.attrs:
        if attr.name == "content_type" and content_types:
            schema = Schema({"enum": list(content_types)}, str)
            # values = ((c.replace('/', '_').upper(), c) for c in content_types)
            # schema = schema_from_type(Enum(f"{name}ContentTypes", values))
            attr = dataclasses.replace(attr, schema=schema)
        elif attr.name == "size":
            attr = dataclasses.replace(attr, schema=int_schema(0, max_size))
        attrs.append(attr)
    meta = dataclasses.replace(DATA_ITEM_META, name=name, attrs=tuple(attrs))
    return meta
