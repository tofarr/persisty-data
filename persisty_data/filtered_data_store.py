from dataclasses import dataclass
from typing import Optional

from persisty.errors import PersistyError
from persisty.store.filtered_store import FilteredStore

from persisty_data.data_item_abc import DataItemABC
from persisty_data.data_store_abc import DataStoreABC


@dataclass(frozen=True)
class FilteredDataStore(DataStoreABC, FilteredStore[DataItemABC]):
    store: DataStoreABC

    def _get_data_writer(self, key: str, content_type: Optional[str], existing_item: Optional[DataItemABC]):
        if not self.search_filter.match(existing_item, self.get_meta().attrs):
            raise PersistyError("update_forbidden")
        return self.store._get_data_writer(key, content_type, existing_item)
