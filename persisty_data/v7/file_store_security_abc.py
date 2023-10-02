from abc import ABC, abstractmethod
from typing import Optional, Generic, TypeVar

from servey.security.authorization import Authorization

from persisty.security.store_access import StoreAccess

_FileStoreABC = "persisty-data.v7.file_store_abc"
T = TypeVar("T")


class FileStoreSecurityABC(ABC, Generic[T]):
    """Object which can be used to wrap a store to add security constraints"""

    @abstractmethod
    def get_secured(
        self, store: _FileStoreABC, authorization: Optional[Authorization]
    ) -> _FileStoreABC:
        """
        Get the access for a store given the authorization
        """

    @abstractmethod
    def get_api_access(self) -> StoreAccess:
        """Get the api access - the max potential access for this store for apis"""
