from typing import Optional, List, Type

from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.store_meta import get_meta
from servey.action.action import action
from servey.security.authorization import Authorization
from servey.trigger.web_trigger import WEB_GET, WEB_POST

from persisty_data.file_handle import FileHandle
from persisty_data.file_store_abc import FileStoreABC
from persisty_data.stored_file_handle import (
    FileHandleSearchFilter,
    FileHandleSearchOrder, StoredFileHandle,
)
from persisty_data.upload_handle import UploadHandle
from persisty_data.upload_part import UploadPart

_ATTRS = get_meta(StoredFileHandle).attrs


def create_action_for_file_read(
    store: FileStoreABC,
    file_handle_type: Type[FileHandle],
    file_handle_result_type: Type,
):
    if store.get_api().get_meta().store_access.read_filter is EXCLUDE_ALL:
        return

    @action(name=f"{store.get_meta().name}_file_read", triggers=WEB_GET)
    def file_read(
        file_name: str, authorization: Optional[Authorization]
    ) -> Optional[file_handle_result_type]:
        secured_store = store.get_secured(authorization)
        item = secured_store.file_read(file_name)
        store_access = secured_store.get_meta().store_access
        return file_handle_result_type(
            item=item,
            updatable=store_access.update_filter.match(item, _ATTRS),
            deletable=store_access.create_filter.match(item, _ATTRS),
        )

    return file_read


def create_action_for_file_read_batch(
    store: FileStoreABC,
    file_handle_type: Type[FileHandle],
    file_handle_result_type: Type,
):
    if store.get_api().get_meta().store_access.read_filter is EXCLUDE_ALL:
        return

    @action(name=f"{store.get_meta().name}_file_read_batch", triggers=WEB_GET)
    def file_read_batch(
        file_names: List[str], authorization: Optional[Authorization]
    ) -> List[Optional[file_handle_result_type]]:
        secured_store = store.get_secured(authorization)
        items = secured_store.file_read_batch(file_names)
        store_access = secured_store.get_meta().store_access
        create_filter = store_access.create_filter
        update_filter = store_access.update_filter
        results = [
            file_handle_result_type(
                item=item,
                updatable=update_filter.match(item, _ATTRS),
                deletable=create_filter.match(item, _ATTRS),
            )
            for item in items
        ]
        return results

    return file_read_batch


def create_action_for_file_count(store: FileStoreABC):
    api_access = store.get_api().get_meta().store_access
    if api_access.read_filter is EXCLUDE_ALL or not api_access.searchable:
        return

    @action(name=f"{store.get_meta().name}_file_count", triggers=WEB_GET)
    def file_count(
        search_filter: FileHandleSearchFilter, authorization: Optional[Authorization]
    ) -> int:
        secured_store = store.get_secured(authorization)
        search_filter = search_filter.to_search_filter()
        result = secured_store.file_count(search_filter)
        return result

    return file_count


def create_action_for_file_search(
    store: FileStoreABC,
    file_handle_type: Type[FileHandle],
    file_handle_result_type: Type,
    file_handle_result_set_type: Type,
):
    api_access = store.get_api().get_meta().store_access
    if api_access.read_filter is EXCLUDE_ALL or not api_access.searchable:
        return

    @action(name=f"{store.get_meta().name}_file_search", triggers=WEB_GET)
    def file_search(
        search_filter: Optional[FileHandleSearchFilter] = None,
        search_order: Optional[FileHandleSearchOrder] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
        authorization: Optional[Authorization] = None,
    ) -> file_handle_result_set_type:
        secured_store = store.get_secured(authorization)
        search_filter = (
            search_filter.to_search_filter() if search_filter else INCLUDE_ALL
        )
        result = secured_store.file_search(search_filter, search_order, page_key, limit)
        store_access = secured_store.get_meta().store_access
        create_filter = store_access.create_filter
        update_filter = store_access.update_filter
        result = file_handle_result_set_type(
            results=[
                file_handle_result_type(
                    item=item,
                    updatable=update_filter.match(item, _ATTRS),
                    deletable=create_filter.match(item, _ATTRS),
                )
                for item in result.results
            ],
            next_page_key=result.next_page_key,
        )
        return result

    return file_search


def create_action_for_file_delete(store: FileStoreABC):
    if store.get_api().get_meta().store_access.delete_filter is EXCLUDE_ALL:
        return

    @action(name=f"{store.get_meta().name}_file_delete", triggers=WEB_POST)
    def file_delete(
        file_name: str, authorization: Optional[Authorization] = None
    ) -> bool:
        secured_store = store.get_secured(authorization)
        return secured_store.file_delete(file_name)

    return file_delete


def _is_not_editable(store: FileStoreABC):
    api_access = store.get_api().get_meta().store_access
    return (
        api_access.create_filter is EXCLUDE_ALL
        and api_access.update_filter is EXCLUDE_ALL
    )


def create_action_for_upload_create(store: FileStoreABC):
    if _is_not_editable(store):
        return

    @action(name=f"{store.get_meta().name}_upload_create", triggers=WEB_POST)
    def upload_create(
        file_name: str,
        content_type: Optional[str] = None,
        size_in_bytes: Optional[int] = None,
        authorization: Optional[Authorization] = None,
    ) -> Optional[UploadHandle]:
        secured_store = store.get_secured(authorization)
        return secured_store.upload_create(file_name, content_type, size_in_bytes)

    return upload_create


def create_action_for_upload_read(store: FileStoreABC):
    if _is_not_editable(store):
        return

    @action(name=f"{store.get_meta().name}_upload_read", triggers=WEB_GET)
    def upload_read(
        upload_id: str, authorization: Optional[Authorization] = None
    ) -> Optional[UploadHandle]:
        secured_store = store.get_secured(authorization)
        return secured_store.upload_read(upload_id)

    return upload_read


def create_action_for_upload_finish(
    store: FileStoreABC,
    file_handle_type: Type[FileHandle],
    file_handle_result_type: Type,
):
    if _is_not_editable(store):
        return

    @action(name=f"{store.get_meta().name}_upload_finish", triggers=WEB_POST)
    def upload_finish(
        upload_id: str, authorization: Optional[Authorization] = None
    ) -> Optional[file_handle_result_type]:
        secured_store = store.get_secured(authorization)
        item = secured_store.upload_finish(upload_id)
        if item:
            store_access = secured_store.get_meta().store_access
            return file_handle_result_type(
                item=item,
                updatable=store_access.update_filter.match(item, _ATTRS),
                deletable=store_access.create_filter.match(item, _ATTRS),
            )

    return upload_finish


def create_action_for_upload_delete(store: FileStoreABC):
    if _is_not_editable(store):
        return

    @action(name=f"{store.get_meta().name}_upload_delete", triggers=WEB_POST)
    def upload_delete(
        upload_id: str, authorization: Optional[Authorization] = None
    ) -> bool:
        secured_store = store.get_secured(authorization)
        return secured_store.upload_delete(upload_id)

    return upload_delete


def create_action_for_upload_part_create(store: FileStoreABC):
    if _is_not_editable(store):
        return

    @action(name=f"{store.get_meta().name}_upload_part_create", triggers=WEB_POST)
    def upload_part_create(
        upload_id: str, authorization: Optional[Authorization] = None
    ) -> Optional[UploadPart]:
        secured_store = store.get_secured(authorization)
        return secured_store.upload_part_create(upload_id)

    return upload_part_create


def create_action_for_upload_part_search(store: FileStoreABC):
    if _is_not_editable(store):
        return

    @action(name=f"{store.get_meta().name}_upload_part_search", triggers=WEB_GET)
    def upload_part_search(
        upload_id__eq: str,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
        authorization: Optional[Authorization] = None,
    ) -> Optional[UploadPart]:
        secured_store = store.get_secured(authorization)
        return secured_store.upload_part_search(upload_id__eq, page_key, limit)

    return upload_part_search


def create_action_for_upload_part_count(store: FileStoreABC):
    if _is_not_editable(store):
        return

    @action(name=f"{store.get_meta().name}_upload_part_count", triggers=WEB_GET)
    def upload_part_count(
        upload_id__eq: str, authorization: Optional[Authorization] = None
    ) -> int:
        secured_store = store.get_secured(authorization)
        return secured_store.upload_part_count(upload_id__eq)

    return upload_part_count
