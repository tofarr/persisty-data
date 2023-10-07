from typing import Iterator

from marshy.factory.impl_marshaller_factory import get_impls
from persisty.finder.store_meta_finder_abc import StoreMetaFinderABC
from persisty.store_meta import StoreMeta

from persisty_data.finder.file_store_finder_abc import FileStoreFinderABC


class FileStoreStoreMetaFinder(StoreMetaFinderABC):
    """Finder for item stores which finds all data stores and extracts the item stores from them"""

    def find_store_meta(self) -> Iterator[StoreMeta]:
        for file_store_finder in get_impls(FileStoreFinderABC):
            for file_store in file_store_finder().find_file_stores():
                yield from file_store.get_persisty_store_meta()
