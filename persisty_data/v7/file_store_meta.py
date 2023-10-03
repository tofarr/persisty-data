from dataclasses import dataclass
from enum import Enum
from typing import Optional

from persisty.security.store_access import StoreAccess, ALL_ACCESS
from persisty.security.store_security import UNSECURED
from servey.cache_control.cache_control_abc import CacheControlABC
from servey.cache_control.secure_hash_cache_control import SecureHashCacheControl

from persisty_data.v7.file_store_security_abc import FileStoreSecurityABC


@dataclass
class FileStoreMeta:
    name: str
    store_security: FileStoreSecurityABC = UNSECURED
    cache_control: CacheControlABC = SecureHashCacheControl()
    store_access: StoreAccess = ALL_ACCESS
    permitted_content_types: Optional[Enum] = None
    max_file_size: int = 256 * 1024 * 1024
    max_part_size: int = 16 * 1024 * 1024
    upload_expire_in: int = 3600
    batch_size: int = 10
    description: Optional[str] = None