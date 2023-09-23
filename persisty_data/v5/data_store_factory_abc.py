from abc import abstractmethod, ABC
from typing import Tuple, Optional

from persisty.security.store_security_abc import StoreSecurityABC
from persisty.store.store_abc import StoreABC


class DataStoreFactoryABC(ABC):

    @abstractmethod
    def create_all(
        self,
        name: str,
        store_security: StoreSecurityABC,
        permitted_content_types: Optional[Tuple[str, ...]] = None,
        max_size_in_bytes: int = 100 * 1024 * 1024,
        expire_in: Optional[int] = None
    ) -> Tuple[StoreABC, ...]:
        """
        create all stores needed to support data : FileHandle, Upload, UploadPart.
        May also require low level chunk stores.
        """