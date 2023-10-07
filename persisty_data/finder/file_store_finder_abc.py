from abc import ABC, abstractmethod
from typing import Iterator

from marshy.factory.impl_marshaller_factory import get_impls

_FileStoreABC = "persisty_data.file_store_abc.FileStoreABC"


class FileStoreFinderABC(ABC):
    @abstractmethod
    def find_file_stores(self) -> _FileStoreABC:
        """Find all available store items"""


def find_file_stores() -> Iterator[_FileStoreABC]:
    names = set()
    for finder in get_impls(FileStoreFinderABC):
        for file_store in finder().find_file_stores():
            name = file_store.get_meta().name
            if name not in names:
                names.add(name)
                yield file_store


def find_file_store_by_name(store_name: str) -> _FileStoreABC:
    result = next(s for s in find_file_stores() if s.get_meta().name == store_name)
    return result
