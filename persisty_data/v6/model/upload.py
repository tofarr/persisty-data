from datetime import datetime
from typing import Optional, Type

from persisty.attr.attr import Attr
from persisty.security.owned_store_security import OwnedStoreSecurity
from persisty.security.store_access import NO_UPDATES
from persisty.security.store_security import StoreSecurity
from persisty.security.store_security_abc import StoreSecurityABC
from persisty.stored import stored
from servey.security.authorization import Authorization

from persisty_data.v6.generator.content_type_generator import ContentTypeGenerator
from persisty_data.v6.generator.future_timestamp_generator import FutureTimestampGenerator
from persisty_data.v6.model.file_handle import FileHandle


class Upload:
    """
    Creatable but not updatable
    """

    id: str
    item_key: str
    content_type: Optional[str] = Attr(
        create_generator=ContentTypeGenerator("item_key")
    )
    max_part_size_in_bytes: int = 256 * 1024  # 256kb
    max_number_of_parts: int = 1024  # Default max size is 256Mb
    subject_id: Optional[str] = None
    expire_at: datetime = Attr(create_generator=FutureTimestampGenerator(3600))
    created_at: datetime

    def finish_upload(self, authorization: Optional[Authorization]) -> FileHandle:
        raise NotImplementedError()


def create_stored_upload_type(
    store_name: str,
    base_type: Type[Upload] = Upload,
    store_security: Optional[StoreSecurityABC] = None,
):
    upload_type = type(store_name.title().replace("_", "") + "Upload", (base_type,), {})
    if not store_security:
        store_security = OwnedStoreSecurity(
            store_security=StoreSecurity(NO_UPDATES, tuple(), NO_UPDATES)
        )
    stored_type = stored(upload_type, store_security=store_security)
    return stored_type
