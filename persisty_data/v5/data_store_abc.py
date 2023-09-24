from abc import abstractmethod, ABC
from typing import Optional

from servey.security.authorization import Authorization

from persisty_data.v5.begin_upload_result import BeginUploadResult


class DataStoreABC(ABC):

    @abstractmethod
    def begin_upload(self, key: Optional[str], content_type: Optional[str], file_size: Optional[int], authorization: Optional[Authorization]) -> BeginUploadResult:
        """ Begin a new upload """

    def search_uploads(self, search_filter: SearchFilterABC[Upload], authorization: Authorization) -> ResultSet[Upload]:
