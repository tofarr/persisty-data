from __future__ import annotations

from abc import ABC, abstractmethod
from io import IOBase
from typing import Optional, List, Iterator

from persisty.result import result_dataclass_for
from persisty.result_set import result_set_dataclass_for, ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store_meta import StoreMeta
from servey.action.action import Action, get_action
from servey.security.authorization import Authorization

from persisty_data.file_handle import FileHandle
from persisty_data.file_store_meta import FileStoreMeta
from persisty_data.stored_file_handle import (
    FileHandleSearchOrder,
    stored_file_handle,
)
from persisty_data.upload_handle import UploadHandle, UploadHandleResultSet
from persisty_data.upload_part import UploadPartResultSet, UploadPart

_Route = "starlette.routing.Route"


# pylint: disable=R0904
class FileStoreABC(ABC):
    """
    Interface representing binary data storage. Methods of this class are typically
    converted directly into servey actions.
    Any underlying store object is typically not externally accessible
    """

    @abstractmethod
    def get_meta(self) -> FileStoreMeta:
        """Get the meta for this store"""

    @abstractmethod
    def get_persisty_store_meta(self) -> Iterator[StoreMeta]:
        """Get any persisty stores associated with this data store"""

    def get_actions(self) -> Iterator[Action]:
        """Get any servey actions associated with this data store"""
        from persisty_data import file_store_actions

        file_handle_type = stored_file_handle(self.get_meta())
        file_handle_result_type = result_dataclass_for(file_handle_type)
        file_handle_result_set_type = result_set_dataclass_for(
            file_handle_result_type, f"{file_handle_type.__name__}ResultSet"
        )

        actions = (
            file_store_actions.create_action_for_file_read(
                self, file_handle_result_type
            ),
            file_store_actions.create_action_for_file_count(self),
            file_store_actions.create_action_for_file_delete(self),
            file_store_actions.create_action_for_file_search(
                self,
                file_handle_result_type,
                file_handle_result_set_type,
            ),
            file_store_actions.create_action_for_file_read_batch(
                self, file_handle_result_type
            ),
            file_store_actions.create_action_for_upload_create(self),
            file_store_actions.create_action_for_upload_delete(self),
            file_store_actions.create_action_for_upload_finish(
                self, file_handle_result_type
            ),
            file_store_actions.create_action_for_upload_part_count(self),
            file_store_actions.create_action_for_upload_part_create(self),
            file_store_actions.create_action_for_upload_part_search(self),
            file_store_actions.create_action_for_upload_read(self),
        )
        actions = (get_action(a) for a in actions if a)
        return iter(actions)

    @abstractmethod
    def get_routes(self) -> Iterator[_Route]:
        """Get any starlette routes associated with this data store"""

    @abstractmethod
    def content_read(self, file_name: str) -> Optional[IOBase]:
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
        part_id: str,
    ) -> Optional[IOBase]:
        """Create a writer to the upload within the store"""

    @abstractmethod
    def file_read(self, file_name: str) -> Optional[FileHandle]:
        """Read a data handle"""

    @abstractmethod
    def file_read_batch(self, file_names: List[str]) -> List[Optional[FileHandle]]:
        """Read a batch of data handles"""
        result = [self.file_read(n) for n in file_names]
        return result

    @abstractmethod
    def file_search(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[FileHandleSearchOrder] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[FileHandle]:
        """Search data handles. Does not support the standard search order as this is not supported by"""

    @abstractmethod
    def file_count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        """Get a count of files matching the search filter given"""

    @abstractmethod
    def file_delete(self, file_name: str) -> bool:
        """Delete a data item"""

    @abstractmethod
    def upload_create(
        self,
        file_name: Optional[str],
        content_type: Optional[str],
        size_in_bytes: Optional[int],
    ) -> Optional[UploadHandle]:
        """Create a new upload handle"""

    @abstractmethod
    def upload_read(self, upload_id: str) -> Optional[UploadHandle]:
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
    def upload_finish(self, upload_id: str) -> Optional[FileHandle]:
        """Finish upload"""

    @abstractmethod
    def upload_delete(self, upload_id: str) -> bool:
        """Delete an upload. Return true if item existed and was deleted"""

    @abstractmethod
    def upload_part_create(self, upload_id: str) -> Optional[UploadPart]:
        """Create a new upload part in the upload given"""

    @abstractmethod
    def upload_part_search(
        self,
        upload_id__eq: str,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> UploadPartResultSet:
        """Search upload parts"""

    @abstractmethod
    def upload_part_count(self, upload_id__eq: str) -> int:
        """Get a count of upload parts"""

    @staticmethod
    def get_json_schema():
        return {}

    def get_secured(self, authorization: Optional[Authorization]):
        return self.get_meta().store_security.get_secured(self, authorization)

    def get_api(self):
        return self.get_meta().store_security.get_api(self)
