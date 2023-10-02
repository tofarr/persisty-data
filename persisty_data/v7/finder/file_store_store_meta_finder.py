from typing import Iterator

from marshy.factory.impl_marshaller_factory import get_impls
from persisty.finder.store_meta_finder_abc import StoreMetaFinderABC
from persisty.store_meta import StoreMeta

from persisty_data.v7.finder.file_store_finder_abc import FileStoreFinderABC


class FileStoreStoredFinder(StoreMetaFinderABC):
    """Finder for item stores which finds all data stores and extracts the item stores from them"""

    def find_store_meta(self) -> Iterator[StoreMeta]:
        for data_store_finder in get_impls(FileStoreFinderABC):
            for data_store in data_store_finder().find_file_stores():
                yield from data_store.get_stored()
