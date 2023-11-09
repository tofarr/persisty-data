from abc import ABC, abstractmethod
from typing import Optional, Generic, TypeVar

from servey.security.authorization import Authorization

_FileStoreABC = "persisty-data.file_store_abc.FileStoreABC"
T = TypeVar("T")


class FileStoreSecurityABC(ABC, Generic[T]):
    """Object which can be used to wrap a store to add security constraints"""

    @abstractmethod
    def get_secured(
        self, file_store: _FileStoreABC, authorization: Optional[Authorization]
    ) -> _FileStoreABC:
        """
        Get the access for a store given the authorization
        """

    @abstractmethod
    def get_api(self, file_store: _FileStoreABC) -> _FileStoreABC:
        """Get the api access - the max potential access for this store for apis"""
