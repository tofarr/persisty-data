from __future__ import annotations
from abc import ABC, abstractmethod
from io import IOBase
from typing import Optional, List
from uuid import UUID

from persisty.result_set import ResultSet
from persisty.security.store_access import StoreAccess
from persisty.util import UNDEFINED
from servey.security.authorization import Authorization

from persisty_data.v7.data_handle import DataHandle
from persisty_data.v7.upload_handle import UploadHandle


class DataStoreABC(ABC):
    """
    Interface representing binary data storage. Methods of this class are typically converted directly into servey actions.
    Any underlying store object is typically not externally accessible
    """

    @abstractmethod
    def get_name(self) -> str:
        """ Get the name of this store """

    def get_access(self) -> StoreAccess:
        """ Get the access level for this store """
    @abstractmethod
    def get_secured(self, authorization: Optional[Authorization] = None) -> DataStoreABC:
        """ Get a secured version of this data store """

    @abstractmethod
    def data_create(self, source: IOBase, key: Optional[str], content_type: Optional[str]):
        """ Create a data handle with data from the source URL """

    @abstractmethod
    def data_read(self, key: str) -> DataHandle:
        """ Read a data handle """

    @abstractmethod
    def data_read_batch(self, keys: List[str]) -> List[Optional[DataHandle]]:
        """ Read a batch of data handles """
    @abstractmethod
    def data_search(self, group_id__eq: Optional[str] = UNDEFINED) -> ResultSet[DataHandle]:
        """ Search data handles """

    @abstractmethod
    def upload_create(
        self,
        key: str,
        group_id: Optional[str],
        content_type: Optional[str],
        size_in_bytes: Optional[int],
    ) -> UploadHandle:
        """ Create a new upload handle """

    @abstractmethod
    def upload_read(
        self,
        upload_id: UUID,
    ) -> UploadHandle:
        """ Read details of an upload. Includes a first page of upload parts. """

    @abstractmethod
    def upload_search(self, group_id__eq: Optional[str] = UNDEFINED, page_key: Optional[str] = None, limit: Optional[int] = None) -> ResultSet[UploadHandle]:
        """ Search uploads """

    @abstractmethod
    def upload_count(self, group_id__eq: Optional[str] = UNDEFINED) -> int:
        """ Search uploads """

    @abstractmethod
    def upload_finish(self, upload_id: str) -> DataHandle:
        """ Finish upload """

    @abstractmethod
    def upload_delete(self, upload_id: str) -> bool:
        """ Delete an upload. Return true if item existed and was deleted """

    @abstractmethod
    def upload_part_create(self, upload_id: str) -> Optional[str]:
        """ Create a new upload part in the upload given """

    @abstractmethod
    def upload_part_search(self, upload_id__eq: str, page_key: Optional[str] = None) -> ResultSet[str]:
        """  Search upload parts """

    @abstractmethod
    def upload_part_count(self, upload_id__eq: str) -> int:
        """ Get a count of upload parts """


