from abc import ABC, abstractmethod
from typing import List, Dict, Optional

from persisty.security.store_security_abc import StoreSecurityABC
from persisty.store_meta import StoreMeta


class DataStoreABC(ABC):

    @abstractmethod
    def create_store_meta(self, name: str, store_security: Optional[StoreSecurityABC] = None) -> List[StoreMeta]:
        """ Create a set of stores for this data store """


def add_stores_for_data(data_store: DataStoreABC, target: Dict):
    for store_meta in data_store.create_store_meta():
        target[store_meta.name] = store_meta.get_stored_dataclass()
