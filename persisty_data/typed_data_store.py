import dataclasses
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, List, Set

from persisty.batch_edit import BatchEdit
from persisty.batch_edit_result import BatchEditResult
from persisty.errors import PersistyError
from persisty.store.wrapper_store_abc import WrapperStoreABC
from persisty.store_meta import StoreMeta
from schemey import schema_from_type

from persisty_data.data_item_abc import DataItemABC
from persisty_data.data_store_abc import DataStoreABC


@dataclass
class TypedDataStore(DataStoreABC, WrapperStoreABC[DataItemABC]):
    store: DataStoreABC
    meta: StoreMeta
    content_types: Set[str]

    def get_store(self) -> DataStoreABC:
        return self.store

    def get_meta(self) -> StoreMeta:
        return self.meta

    def _check_item(self, item: DataItemABC):
        self._check_content_type(item.content_type)

    def _check_content_type(self, content_type: Optional[str]):
        if content_type not in self.content_types:
            raise PersistyError(f"invalid_content_type:{content_type}")

    def create(self, item: DataItemABC) -> Optional[DataItemABC]:
        self._check_item(item)
        return self.store.create(item)

    # pylint: disable=W0212
    def _update(
        self, key: str, item: DataItemABC, updates: DataItemABC
    ) -> Optional[DataItemABC]:
        self._check_item(updates)
        return self.store._update(key, item, updates)

    # pylint: disable=W0212
    def _edit_batch(
        self,
        edits: List[BatchEdit[DataItemABC, DataItemABC]],
        items_by_key: Dict[str, DataItemABC],
    ) -> List[BatchEditResult[DataItemABC, DataItemABC]]:
        for edit in edits:
            if edit.create_item:
                self._check_item(edit.create_item)
            elif edit.update_item:
                self._check_item(edit.update_item)
        filtered_results = self.store._edit_batch(edits, items_by_key)
        return filtered_results

    def get_data_writer(self, key: str, content_type: Optional[str] = None):
        self._check_content_type(content_type)
        return self.store.get_data_writer(key, content_type)

    def copy_data_from(self, source: DataItemABC):
        self._check_item(source)
        return self.store.copy_data_from(source)
