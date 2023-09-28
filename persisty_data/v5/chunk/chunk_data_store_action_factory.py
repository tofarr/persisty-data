import math
from abc import ABC, abstractmethod
from dataclasses import field, dataclass
from datetime import datetime
from typing import Optional, Callable
from uuid import UUID

from dateutil.relativedelta import relativedelta
from persisty.batch_edit import BatchEdit
from persisty.errors import PersistyError
from persisty.finder.stored_finder_abc import find_stored_by_name
from persisty.security.store_access import StoreAccess
from persisty.security.store_security_abc import StoreSecurityABC
from persisty.servey.action_factory import ActionFactory
from persisty.servey.action_factory_abc import ActionFactoryABC
from persisty.store_meta import StoreMeta
from servey.action.action import get_action, Action, action
from servey.security.authorization import Authorization

from persisty_data.v5.begin_upload_result import BeginUploadResult
from persisty_data.v5.file_handle import FileHandle
from persisty_data.v5.upload_part import UploadPart


@dataclass
class ChunkDataStoreActionFactory(ActionFactoryABC):
    """Create regular actions for a read only file handle store as well as the upload specific actions..."""
    action_factory: ActionFactoryABC = field(default_factory=ActionFactory)




def action_for_upload_create(
    store_meta: StoreMeta,
    max_part_size: int,
    expires_in: int,
    store_security: StoreSecurityABC,
    create_part: Callable[[UUID, int], UploadPart]
) -> Action:
    store_name = store_meta.name
    content_type_attr = next(a for a in store_meta.attrs if a.name == "content_type")
    content_type_type = content_type_attr.schema.python_type

    @action(name=f"{store_name}_upload_create")
    def upload_create(key: Optional[str], content_type: content_type_type, size_in_bytes: Optional[int] = None, authorization: Optional[Authorization] = None) -> BeginUploadResult:
        f""" Begin a new upload to the {store_name} store """

        file_handle_store_meta = find_stored_by_name(store_name)
        secured_store = store_security.get_secured(file_handle_store_meta.create_store(), authorization)
        store_access = secured_store.get_meta().store_access
        file_handle = secured_store.get_stored_dataclass()(
            key=key,
            content_type=content_type,
            size_in_bytes=size_in_bytes
        )
        if not store_access.create_filter.match(file_handle, file_handle_store_meta.attrs):
            raise PersistyError('forbidden')

        upload_store_meta = find_stored_by_name(f"{store_name}_upload")
        upload_part_store_meta = find_stored_by_name(f"{store_name}_upload_part")

        number_of_parts = 1
        if size_in_bytes:
            number_of_parts = math.ceil(size_in_bytes / max_part_size)

        max_size_in_bytes = get_max_size_in_bytes(file_handle_store_meta)

        upload = upload_store_meta.get_create_dataclass()(
            item_key=key,
            content_type=content_type,
            max_number_of_parts=math.ceil(max_size_in_bytes / max_part_size),
            expire_at=datetime.now() + relativedelta(seconds=expires_in)
        )
        upload = upload_store_meta.create_store().create(upload)
        edits = (
            BatchEdit(create_item=create_part(upload.id, part_number))
            for part_number in range(1, number_of_parts+1)
        )
        edit_results = upload_part_store_meta.create_store().edit_all(edits)
        parts = [e.create_item for e in edit_results]
        return BeginUploadResult(upload, parts)

    return get_action(upload_create)


def action_for_upload_finish() -> Action:
    """ Finish an upload to this data store """

    def upload_finish(upload_id: UUID, authorization: Optional[Authorization] = None) -> FileHandle:


def action_for_upload_search(self, search_filter: SearchFilterABC[Upload], search_order: SearchOrder[Upload], page_key: str, limit: Optional[int] = None, authorization: Optional[Authorization] = None) -> ResultSet[Result[Upload]]:
    """ Search uploads of this data store """


def upload_count(self, search_filter: SearchFilterABC[Upload], authorization: Optional[Authorization] = None) -> int:
    """ Count uploads to this data store """


def upload_part_create(self, upload_id: int, authorization: Optional[Authorization] = None):
    """ """


def upload_part_search(self, search_filter: SearchFilterABC[Upload], search_order: SearchOrder[Upload], page_key: str, limit: Optional[int] = None, authorization: Optional[Authorization] = None) -> ResultSet[Result[UploadPart]]:
    """ """


def upload_part_count(self, search_filter: SearchFilterABC[Upload], authorization: Optional[Authorization] = None) -> int:
    """ """
