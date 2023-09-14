from dataclasses import dataclass
from typing import Optional

from persisty.errors import PersistyError
from persisty.store.restrict_access_store import RestrictAccessStore

from persisty_data.data_item_abc import DataItemABC
from persisty_data.data_store_abc import DataStoreABC, HostingWrapper


@dataclass
class RestrictAccessDataStore(DataStoreABC, RestrictAccessStore[DataItemABC]):
    store: DataStoreABC

    def _get_data_writer(self, key: str, content_type: Optional[str], existing_item: Optional[DataItemABC]):
        store_access = self.get_store_access()
        if not (store_access.updatable if existing_item else store_access.creatable):
            raise PersistyError('unavailable_operation')
        return self.store._get_data_writer(key, content_type, existing_item)

    def get_hosting_wrapper(self) -> HostingWrapper:
        return self.store.get_hosting_wrapper()
