from typing import Optional, Tuple

from persisty.security.store_security_abc import StoreSecurityABC
from persisty.store.store_abc import StoreABC

from persisty_data.v5.data_store_factory_abc import DataStoreFactoryABC


class ChunkDataStoreFactory(DataStoreFactoryABC):
    def create_all(
        self,
        name: str,
        store_security: StoreSecurityABC,
        permitted_content_types: Optional[Tuple[str, ...]] = None,
        max_size_in_bytes: int = 100 * 1024 * 1024,
        expire_in: Optional[int] = None
    ) -> Optional[Tuple[StoreABC, ...]]:
        # create chunk store - no external access - action has reaper action
        # create chunk upload part store - external access based on passed in security - hide stream id
        # create chunk upload store - has external access based on passed in security - hide stream id
        # create chunk file handle store - has external access based on passed in security - hide stream id

        pass