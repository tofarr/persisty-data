from abc import ABC
from typing import Optional
from uuid import UUID

from persisty.servey.action_factory_abc import ActionFactoryABC


class DataStoreActionFactoryABC(ActionFactoryABC, ABC):
    """ Create regular actions as well as the following... """

    def begin_upload(self, key: Optional[str], content_type: Optional[str], file_size: Optional[int]):
        pass  # Begin an upload and create a single part if file size not specified. return the upload parts

    def finish_upload(self, upload_id: UUID):
        pass  # create file handle

    def abort_upload(self, upload_id: UUID):
        pass

    def copy_to_store(self, url: str, key: Optional[str] = None):
        pass