from abc import abstractmethod, ABC
from io import IOBase
from pathlib import Path
from typing import Union, Optional, List, Iterable, Iterator

from persisty.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.store.store_abc import StoreABC

from persisty_data.upload_form import UploadForm
from persisty_data.v2.data_handle import DataHandle


class DataStoreABC(StoreABC[DataHandle], ABC):
    @abstractmethod
    def read(self, key: str) -> Optional[DataHandle]:
        """Get the data handle for the key given (if present)"""

    def read_batch(self, keys: List[str]) -> List[Optional[DataHandle]]:
        """Read a batch of data handles from this store"""
        result = [self.read(key) for key in keys]
        return result

    def read_all(
        self, keys: Union[Iterable[str], Iterator[str]]
    ) -> Iterator[DataHandle]:
        for key in keys:
            yield self.read(key)

    @abstractmethod
    def search(
        self,
        search_filter: SearchFilterABC[DataHandle] = INCLUDE_ALL,
        search_order: Optional[SearchOrder[DataHandle]] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[DataHandle]:
        """Search the store for data handles"""

    @abstractmethod
    def search_all(
        self,
        search_filter: SearchFilterABC[DataHandle] = INCLUDE_ALL,
        search_order: Optional[SearchOrder[DataHandle]] = None,
    ) -> Iterator[DataHandle]:
        """Search the store for data handles"""

    @abstractmethod
    def count(self, search_filter: SearchFilterABC[DataHandle] = INCLUDE_ALL) -> int:
        """Get a count from the store"""

    @abstractmethod
    def create(
        self,
        key: str,
        data: Union[Path, str, IOBase, bytearray],
        content_type: Optional[str] = None,
    ):
        """Create a new entry in the store"""

    @abstractmethod
    def begin_input(
        self,
        key: str,
        content_type: Optional[str] = None,
        timeout: Optional[int] = 3600,
    ) -> str:
        """Start creating a new item in the store. Return an input id."""

    @abstractmethod
    def add_input(
        self,
        input_key: str,
        part_number: int,
        data: Union[Path, str, IOBase, bytearray],
    ):
        """Create a new multipart input in the store."""

    @abstractmethod
    def finish_input(self, input_key: str) -> str:
        """Convert an input into an item in the store."""

    @abstractmethod
    def cancel_input(self, input_key: str):
        """Abandon an input"""

    @abstractmethod
    def read_data(self, key: str) -> Optional[IOBase]:
        """Get the data in the item with the key given as an input stream"""

    @abstractmethod
    def read_data_to(
        self,
        key: str,
        output: Union[Path, str, IOBase, bytearray],
        offset: int = 0,
        max_length: Optional[int] = None,
    ):
        """
        Read data from the item with the key given to the output given.
        Throw a PersistyDataError error if not found / readable
        """

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete an item from the store"""

    def delete_batch(self, keys: List[str]) -> List[bool]:
        results = [self.delete(key) for key in keys]
        return results

    def delete_all(self, keys: Union[Iterable[str], Iterator[str]]) -> Iterator[bool]:
        for key in keys:
            yield self.delete(key)

    @abstractmethod
    def get_download_url(self, key: str) -> str:
        """Get the download url for the item with the key given"""

    def get_download_url_batch(self, keys: List[str]) -> List[Optional[str]]:
        results = [self.get_download_url(key) for key in keys]
        return results

    def get_all_download_urls(
        self, keys: Union[Iterable[str], Iterator[str]]
    ) -> Iterator[str]:
        for key in keys:
            yield self.get_download_url(key)

    @abstractmethod
    def create_upload_form(self, key: Optional[str]) -> UploadForm:
        """Create an upload form"""
