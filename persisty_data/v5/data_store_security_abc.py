from abc import ABC, abstractmethod

from persisty.security.store_security_abc import StoreSecurityABC


class DataStoreSecurityABC(ABC):
    # store security...
    #   create translates to create, read, delete, search, on upload & upload part
    #   read, delete, search translates directly on file handle

    @abstractmethod
    def get_file_handle_security(self) -> StoreSecurityABC:
        """Get the security for file handles"""

    @abstractmethod
    def get_upload_security(self) -> StoreSecurityABC:
        """Get the security for file handles"""

    @abstractmethod
    def get_upload_part_security(self) -> StoreSecurityABC:
        """Get the security for file handles"""
