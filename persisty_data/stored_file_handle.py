from dataclasses import dataclass, field
from typing import Dict, Type

from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.search_filter_factory import search_filter_dataclass_for
from persisty.search_order.search_order_factory import search_order_dataclass_for
from persisty.store_meta import get_meta
from persisty.stored import stored
from schemey import schema_from_type, SchemaContext
from schemey.factory.dataclass_schema_factory import DataclassSchemaFactory
from schemey.schema import int_schema, Schema

from persisty_data.file_handle import FileHandle
from persisty_data.file_store_meta import FileStoreMeta

StoredFileHandle = stored(FileHandle, key_config=AttrKeyConfig("file_name"))
FileHandleSearchFilter = search_filter_dataclass_for(get_meta(StoredFileHandle))
FileHandleSearchOrder = search_order_dataclass_for(get_meta(StoredFileHandle))


def stored_file_handle(file_store_meta: FileStoreMeta):
    name = file_store_meta.name.title().replace("_", "")

    # noinspection PyDecorator
    @classmethod
    def _schema_factory(
        cls, context: SchemaContext, path: str, ref_schemas: Dict[Type, Schema]
    ):
        schema = DataclassSchemaFactory().from_type(cls, context, path, ref_schemas)
        schema.schema["persistyData"] = {
            "store_name": file_store_meta.name,
            "creatable": file_store_meta.store_access.create_filter is not EXCLUDE_ALL
        }
        return schema

    annotations = {"size_in_bytes": int}
    params = {
        "__annotations__": annotations,
        "size_in_bytes": field(
            metadata={"schemey": int_schema(maximum=file_store_meta.max_file_size)}
        ),
        "__schema_factory__": _schema_factory,
    }
    if file_store_meta.permitted_content_types:
        annotations["content_type"] = str
        # noinspection PyTypeChecker
        params["content_type"] = field(
            metadata={
                "schemey": schema_from_type(file_store_meta.permitted_content_types)
            }
        )

    # noinspection PyTypeChecker
    result = dataclass(type(name, (FileHandle,), params))
    return result
