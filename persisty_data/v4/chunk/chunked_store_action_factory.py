import math
from abc import ABC, abstractmethod
from dataclasses import field, dataclass
from datetime import datetime
from typing import Iterator, Optional
from uuid import uuid4

from dateutil.relativedelta import relativedelta
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.servey.action_factory import ActionFactory
from persisty.servey.action_factory_abc import ActionFactoryABC, ROUTE
from persisty.store.store_abc import StoreABC
from persisty.util import UNDEFINED
from servey.action.action import Action, action, get_action
from servey.security.authorization import Authorization
from servey.security.authorizer.authorizer_abc import AuthorizerABC
from servey.security.authorizer.authorizer_factory_abc import get_default_authorizer
from servey.trigger.web_trigger import WEB_POST

from persisty_data.v5.chunked_upload import ChunkedUpload
from persisty_data.v5.file_handle import FileHandle
from persisty_data.v5.file_handle_status import FileHandleStatus


@dataclass
class ChunkedStoreActionFactoryABC(ActionFactoryABC, ABC):
    download_path: str
    upload_path: str
    action_factory: ActionFactoryABC = field(default_factory=ActionFactory)
    chunk_size: int = 20 * 1024 * 1024
    expire_in: int = 3600
    authorizer: AuthorizerABC = field(default_factory=get_default_authorizer)

    def create_actions(self, store: StoreABC[FileHandle]) -> Iterator[Action]:
        store_access = store.get_meta().store_security.get_api_access()
        editable = (store_access.create_filter is not EXCLUDE_ALL or store_access.update_filter is not EXCLUDE_ALL)
        if editable:
            yield self.action_for_begin_upload(store)
            yield self.action_for_finish_upload(store)
            yield self.action_for_abort_upload(store)

    def action_for_begin_upload(self, store: StoreABC[FileHandle]) -> Action:
        store_meta = store.get_meta()

        @action(
            name=f"{store_meta.name}_begin_upload",
            description=f"Begin a new upload to {store_meta.name}",
            triggers=WEB_POST
        )
        def begin_upload(
            file_size: int,
            key: Optional[str] = None,
            content_type: Optional[str] = None,
            authorization: Optional[Authorization] = None
        ):
            file_handle = FileHandle(
                key=key or UNDEFINED,
                status=FileHandleStatus.WORKING,
                content_type=content_type
            )
            secured_store = store_meta.create_secured_store(authorization)
            item = secured_store.create(file_handle)
            num_chunks = math.floor(file_size / self.chunk_size)
            if num_chunks * self.chunk_size - file_size:
                num_chunks += 1
            upload_id = str(uuid4())
            expire_at = datetime.now() + relativedelta(seconds=self.expire_in)
            chunked_upload = ChunkedUpload(
                id=upload_id,
                store_name=store_meta.name,
                key=item.key,
                expire_at=expire_at,
                presigned_urls=[
                    self.create_signed_upload_url(store_meta.name, upload_id, chunk_number, expire_at)
                    for chunk_number in range(num_chunks)
                ]
            )
            return chunked_upload

        return get_action(begin_upload)

    @abstractmethod
    def action_for_finish_upload(self, store: StoreABC[FileHandle]) -> Action:
        store_meta = store.get_meta()
        store


    @abstractmethod
    def action_for_abort_upload(self, store: StoreABC[FileHandle]) -> Action:
        """ Create an action for creating an upload """

    def create_routes(self, store: StoreABC[FileHandle]) -> Iterator[ROUTE]:
        from persisty_data.v4.hosted.hosted_store_route_factory import create_route_for_download, create_route_for_upload
        store_meta = store.get_meta()
        yield create_route_for_download(self.download_path, store_meta, self.authorizer)
        yield create_route_for_upload(self.upload_path, store_meta, self.authorizer)
