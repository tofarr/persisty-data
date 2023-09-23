import hashlib
from abc import ABC, abstractmethod
from typing import Iterator, Optional, Callable

from persisty.store.store_abc import StoreABC
from persisty_data.data_item_abc import DataItemABC
from persisty_data.upload_form import UploadForm


# _DataStoreFactoryABC = "persisty_data.DataStoreFactoryABC"
# HostingWrapper = Callable[[_DataStoreFactoryABC], _DataStoreFactoryABC]


class DataStoreABC(StoreABC[DataItemABC], ABC):
    def get_data_writer(self, key: str, content_type: Optional[str] = None):
        existing_item = self.read(key)
        writer = self._get_data_writer(key, content_type, existing_item)
        return writer

    @abstractmethod
    def _get_data_writer(
        self,
        key: str,
        content_type: Optional[str],
        existing_item: Optional[DataItemABC],
    ):
        """Get a data writer - internal call where existing item is already loaded if present."""

    def copy_data_from(self, source: DataItemABC):
        """
        Copy the data from the item given into this data store - implementations may use OS features to speed this up
        """
        with source.get_data_reader() as reader:
            with self.get_data_writer(source.key, source.content_type) as writer:
                copy_data(reader, writer)

    @abstractmethod
    def get_upload_form(self, key: Optional[str] = None) -> UploadForm:
        """
        Get the upload form for the item with the key given. (No key may be provided in the case of create operations)
        """

    def get_download_url(self, key: str) -> Optional[str]:
        """
        Get the download url for the item with the key given
        """

    # def get_hosting_wrapper(self) -> HostingWrapper:
    #    from persisty_data.hosted_data_store_factory import hosted_data_store_factory
    #    return hosted_data_store_factory


# def find_data_stores() -> Iterator[DataStoreABC]:
#    yield from (s for s in find_store() if isinstance(s, DataStoreABC))


def copy_data(reader, writer, buffer_size: int = 64 * 1024):
    while True:
        buffer = reader.read(buffer_size)
        if not buffer:
            return
        writer.write(buffer)


def calculate_etag(reader, buffer_size: int = 64 * 1024) -> str:
    md5 = hashlib.md5()
    while True:
        bytes_ = reader.read(buffer_size)
        if not bytes_:
            break
        md5.update(bytes_)
    result = md5.hexdigest()
    return result
