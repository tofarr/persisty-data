from typing import Optional, Tuple

from persisty.security.named_permission import NamedPermission
from persisty.security.store_access import StoreAccess, ALL_ACCESS
from servey.security.authorization import Authorization

from persisty_data.security.file_store_security_abc import (
    FileStoreSecurityABC,
    _FileStoreABC,
)


class FileStoreSecurity(FileStoreSecurityABC):
    default_access: StoreAccess
    scoped_permissions: Tuple[NamedPermission, ...] = tuple()
    api_access: StoreAccess = ALL_ACCESS

    def get_secured(
        self, file_store: _FileStoreABC, authorization: Optional[Authorization]
    ) -> _FileStoreABC:
        meta = file_store.get_meta()
        store_access = self.get_access(meta.name, authorization)
        if store_access == ALL_ACCESS:
            return file_store
        from persisty_data.security.restrict_access_file_store import (
            RestrictAccessFileStore,
        )

        return RestrictAccessFileStore(file_store, store_access)

    def get_api(self, file_store: _FileStoreABC) -> _FileStoreABC:
        from persisty_data.security.restrict_access_file_store import (
            RestrictAccessFileStore,
        )

        if self.api_access == ALL_ACCESS:
            return file_store

        return RestrictAccessFileStore(file_store, self.api_access)


UNSECURED_FILE = FileStoreSecurity()
