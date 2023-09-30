from datetime import datetime
from io import IOBase
from pathlib import Path
from typing import Optional, Type, BinaryIO
from urllib.request import urlopen

from persisty.attr.attr import Attr
from persisty.errors import PersistyError
from persisty.security.store_access import READ_ONLY
from persisty.security.store_security import StoreSecurity
from persisty.security.store_security_abc import StoreSecurityABC
from persisty.servey.action_factory_abc import ActionFactoryABC
from persisty.stored import stored
from schemey.schema import int_schema
from servey.security.authorization import Authorization

from persisty_data.v6.generator.content_type_generator import ContentTypeGenerator
from persisty_data.v6.generator.pattern_generator import PatternGenerator
from persisty_data.v6.model.upload import Upload


class FileHandle:
    """
    File handle - not directly creatable or updatable.
    Can limit allowed content types using schema.
    Can limit max size in bytes using schema too. (default is 256Mb)
    Download urls may by signed depending on the implementation
    """

    key: str
    content_type: Optional[str] = Attr(create_generator=ContentTypeGenerator())
    size_in_bytes: int = Attr(schema=int_schema(maximum=256 * 1024 * 1024))
    etag: str
    download_url: str = Attr(
        creatable=False,
        create_generator=PatternGenerator("key", "/data/{store_name}/{value}"),
    )
    subject_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    expire_at: Optional[datetime] = None

    def get_reader(self) -> IOBase:
        """
        Get a reader for this file (May read locally or remote - default implementation tries to read download url
        """
        download_url = self.download_url
        if isinstance(download_url, str):
            # noinspection HttpUrlsUsage
            if not download_url.startswith("http://") and not download_url.startswith(
                "https://"
            ):
                raise PersistyError("invalid_download_url")
            return urlopen(download_url)
        if isinstance(download_url, Path):
            # noinspection PyTypeChecker
            return open(download_url, "rb")


def create_stored_file_handle_type(
    store_name: str,
    base_type: Type[FileHandle] = FileHandle,
    store_security: Optional[StoreSecurityABC] = None,
    action_factory: Optional[ActionFactoryABC] = None,
):
    file_handle_type = type(store_name.title().replace("_", ""), (base_type,), {})
    if not store_security:
        store_security = StoreSecurity(READ_ONLY, tuple(), READ_ONLY)
    if not action_factory:
        from persisty_data.v6.action_factory.file_handle_action_factory import (
            FileHandleActionFactory,
        )

        action_factory = FileHandleActionFactory()
    stored_type = stored(
        file_handle_type, store_security=store_security, action_factory=action_factory
    )
    return stored_type
