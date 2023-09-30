from abc import ABC, abstractmethod
from typing import Iterator

from marshy.factory.impl_marshaller_factory import get_impls

from persisty_data.v7.data_store_abc import DataStoreABC


class DataStoreFinderABC(ABC):

    @abstractmethod
    def find_data_stores(self) -> DataStoreABC:
        """Find all available store items"""


def find_data_stores() -> Iterator[DataStoreABC]:
    names = set()
    for finder in get_impls(DataStoreFinderABC):
        for store_meta in finder().find_stored():
            name = store_meta.name
            if name not in names:
                names.add(name)
                yield store_meta


def find_data_store_by_name(store_name: str) -> DataStoreABC:
    result = next(s for s in find_data_stores() if s.name == store_name)
    return result
