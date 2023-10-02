from abc import ABC, abstractmethod
from typing import Iterator

from marshy.factory.impl_marshaller_factory import get_impls

from persisty_data.v7.file_store_abc import FileStoreABC


class FileStoreFinderABC(ABC):
    @abstractmethod
    def find_file_stores(self) -> FileStoreABC:
        """Find all available store items"""


def find_file_stores() -> Iterator[FileStoreABC]:
    names = set()
    for finder in get_impls(FileStoreFinderABC):
        for store_meta in finder().find_stored():
            name = store_meta.name
            if name not in names:
                names.add(name)
                yield store_meta


def find_file_store_by_name(store_name: str) -> FileStoreABC:
    result = next(s for s in find_file_stores() if s.name == store_name)
    return result
