from abc import abstractmethod, ABC
from typing import Optional, List
from uuid import UUID

from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.store.store_abc import StoreABC
from servey.action.action import Action
from servey.security.authorization import Authorization

from persisty_data.v4.file_handle import FileHandle
from persisty_data.v5.begin_upload_result import BeginUploadResult
from persisty_data.v5.upload import Upload


class DataStoreABC(ABC):
    """
    External Interface for a data store
    """

    def get_name(self) -> str:
        """Get the name of this data store"""

    @abstractmethod
    def upload_create(
        self,
        key: Optional[str],
        content_type: Optional[str],
        file_size: Optional[int],
        authorization: Optional[Authorization] = None,
    ) -> BeginUploadResult:
        """Begin a new upload to this data store"""

    @abstractmethod
    def upload_finish(
        self, upload_id: UUID, authorization: Optional[Authorization] = None
    ) -> FileHandle:
        """Finish an upload to this data store"""

    @abstractmethod
    def upload_search(
        self,
        search_filter: SearchFilterABC[Upload],
        search_order: SearchOrder[Upload],
        page_key: str,
        limit: Optional[int] = None,
        authorization: Optional[Authorization] = None,
    ) -> ResultSet[Result[Upload]]:
        """Search uploads of this data store"""

    @abstractmethod
    def upload_count(
        self,
        search_filter: SearchFilterABC[Upload],
        authorization: Optional[Authorization] = None,
    ) -> int:
        """Count uploads to this data store"""

    @abstractmethod
    def upload_part_create(
        self, upload_id: int, authorization: Optional[Authorization] = None
    ):
        """ """

    @abstractmethod
    def upload_part_search(
        self,
        search_filter: SearchFilterABC[Upload],
        search_order: SearchOrder[Upload],
        page_key: str,
        limit: Optional[int] = None,
        authorization: Optional[Authorization] = None,
    ) -> ResultSet[Result[UploadPart]]:
        """ """

    @abstractmethod
    def upload_part_count(
        self,
        search_filter: SearchFilterABC[Upload],
        authorization: Optional[Authorization] = None,
    ) -> int:
        """ """

    @abstractmethod
    def copy_from_store(self, source_key: str, destination: IOBase):
        """Read data to the key given in the current store"""

    @abstractmethod
    def copy_to_store(
        self, source: Callable[[], IOBase], upload_id: UUID, part_number: int
    ):
        """Write data from the source given to the store under the key given"""

    @abstractmethod
    def get_file_handle_store(self) -> StoreABC[FileHandle]:
        """Get the (read only) file handle store"""

    def get_actions(self) -> List[Action]:
        """ """
