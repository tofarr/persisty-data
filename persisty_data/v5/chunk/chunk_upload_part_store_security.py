from typing import Optional

from persisty.security.store_access import StoreAccess, NO_UPDATES
from persisty.security.store_security_abc import StoreSecurityABC, _StoreABC
from servey.security.authorization import Authorization


class ChunkUploadPartStoreSecurity(StoreSecurityABC):
    def get_secured(self, store: _StoreABC, authorization: Optional[Authorization]) -> _StoreABC:
        this wont work because it will not filter meta

    def get_api_access(self) -> StoreAccess:
        return NO_UPDATES