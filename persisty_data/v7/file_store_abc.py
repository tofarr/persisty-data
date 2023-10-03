from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from io import IOBase
from typing import Optional, List, Iterator
from uuid import UUID

from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.store_meta import StoreMeta
from servey.action.action import Action, action, get_action
from servey.security.authorization import Authorization
from servey.trigger.web_trigger import WebTrigger, WebTriggerMethod

from persisty_data.v7.file_handle import (
    FileHandle,
    FileHandleSearchFilter,
    FileHandleSearchOrder,
    FileHandleResultSet,
)
from persisty_data.v7.upload_handle import UploadHandle, UploadHandleResultSet
from persisty_data.v7.upload_part import UploadPartResultSet, UploadPart

_Route = "starlette.routing.Route"


class FileStoreABC(ABC):
    """
    Interface representing binary data storage. Methods of this class are typically converted directly into servey actions.
    Any underlying store object is typically not externally accessible
    """

    @abstractmethod
    def get_meta(self):
        """Get the meta for this store"""

    @abstractmethod
    def get_persisty_store_meta(self) -> Iterator[StoreMeta]:
        """Get any persisty stores associated with this data store"""

    def get_actions(self) -> Iterator[Action]:
        """Get any servey actions associated with this data store"""
        store_access = self.get_meta().get_api_access()
        fn_names = []
        if store_access.read_filter is not EXCLUDE_ALL:
            fn_names.extend(("file_read", "file_read_batch"))
            if store_access.searchable:
                fn_names.extend(("file_search", "file_count"))
        if (
            store_access.create_filter is not EXCLUDE_ALL
            or store_access.update_filter is not EXCLUDE_ALL
        ):
            fn_names.extend(
                (
                    "upload_create",
                    "upload_read",
                    "upload_search",
                    "upload_count",
                    "upload_finish",
                    "upload_delete",
                    "upload_part_create",
                    "upload_part_search",
                    "upload_part_count",
                )
            )
        if store_access.delete_filter is not EXCLUDE_ALL:
            fn_names.append("file_delete")

        for fn_name in fn_names:
            yield self._file_store_action(fn_name)

    def _file_store_action(self, fn_name: str):
        sig = inspect.signature(getattr(self, fn_name))
        params = list(sig.parameters)
        params.append(
            inspect.Parameter(
                name="authorization",
                kind=inspect.Parameter.KEYWORD_ONLY,
                default=None,
                annotation=Optional[Authorization],
            )
        )
        sig = sig.replace(parameters=params)

        def action_fn(**kwargs):
            authorization = kwargs.pop("authorization", None)
            secured_store = self.get_meta().store_security.get_secured(authorization)
            fn = getattr(secured_store, fn_name)
            result = fn.invoke(**kwargs)
            return result

        action_fn.__signature__ = sig
        method = WebTriggerMethod.GET
        action_path = (
            fn_name.replace("-read", "")
            .replace("-create", "")
            .replace("-delete", "")
            .replace("_", "-")
        )
        path = f"/actions/{self.get_meta().name}/{action_path}"
        if fn_name.endswith("_delete"):
            method = WebTriggerMethod.DELETE
        elif fn_name.endswith("create"):
            method = WebTriggerMethod.POST
        action_fn = action(action_fn, triggers=WebTrigger(method))
        return get_action(action_fn)

    @abstractmethod
    def get_routes(self) -> Iterator[_Route]:
        """Get any starlette routes associated with this data store"""

    @abstractmethod
    def content_read(
        self, file_name: str, content_type: Optional[str] = None
    ) -> Optional[IOBase]:
        """Create a reader from the named file within the store"""

    @abstractmethod
    def content_write(
        self,
        file_name: Optional[str],
        content_type: Optional[str],
    ) -> IOBase:
        """Create a writer to the named file within the store"""

    @abstractmethod
    def upload_write(
        self,
        upload_id: UUID,
        part_number: int = 1
    ) -> Optional[IOBase]:
        """Create a writer to the upload within the store"""

    @abstractmethod
    def file_read(self, file_name: str) -> FileHandle:
        """Read a data handle"""

    @abstractmethod
    def file_read_batch(self, file_names: List[str]) -> List[Optional[FileHandle]]:
        """Read a batch of data handles"""
        result = [self.file_read(n) for n in file_names]
        return result

    @abstractmethod
    def file_search(
        self,
        search_filter: Optional[FileHandleSearchFilter] = None,
        search_order: Optional[FileHandleSearchOrder] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> FileHandleResultSet:
        """Search data handles. Does not support the standard search order as this is not supported by"""

    @abstractmethod
    def file_count(self, search_filter: Optional[FileHandleSearchFilter] = None) -> int:
        """Get a count of files matching the search filter given"""

    @abstractmethod
    def file_delete(self, file_name: str) -> bool:
        """Delete a data item"""

    @abstractmethod
    def upload_create(
        self,
        file_name: str,
        content_type: Optional[str],
        size_in_bytes: Optional[int],
    ) -> UploadHandle:
        """Create a new upload handle"""

    @abstractmethod
    def upload_read(self, upload_id: UUID) -> Optional[UploadHandle]:
        """Read details of an upload. Includes a first page of upload parts."""

    @abstractmethod
    def upload_search(
        self,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> UploadHandleResultSet:
        """Search uploads"""

    @abstractmethod
    def upload_count(self) -> int:
        """Search uploads"""

    @abstractmethod
    def upload_finish(self, upload_id: UUID) -> Optional[FileHandle]:
        """Finish upload"""

    @abstractmethod
    def upload_delete(self, upload_id: UUID) -> bool:
        """Delete an upload. Return true if item existed and was deleted"""

    @abstractmethod
    def upload_part_create(self, upload_id: UUID) -> Optional[UploadPart]:
        """Create a new upload part in the upload given"""

    @abstractmethod
    def upload_part_search(
        self,
        upload_id__eq: UUID,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> UploadPartResultSet:
        """Search upload parts"""

    @abstractmethod
    def upload_part_count(self, upload_id__eq: UUID) -> int:
        """Get a count of upload parts"""
