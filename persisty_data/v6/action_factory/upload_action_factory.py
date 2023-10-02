from dataclasses import dataclass, field
from typing import Iterator, Optional

from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.servey.action_factory import ActionFactory
from persisty.servey.action_factory_abc import ActionFactoryABC
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta
from servey.action.action import Action, action, get_action
from servey.security.authorization import Authorization
from servey.trigger.web_trigger import WEB_POST
from starlette.routing import Route

from persisty_data.v6.action_factory.file_handle_action_factory import (
    create_action_for_after_delete,
)
from persisty_data.v6.action_factory.reaper_action import create_reaper_action
from persisty_data.v6.chunk.chunk_file_handle import ChunkFileHandle
from persisty_data.v6.chunk.chunk_upload import ChunkUpload


@dataclass
class UploadActionFactory(ActionFactoryABC):
    action_factory: ActionFactoryABC = field(default_factory=ActionFactory)

    def create_actions(self, store: StoreABC) -> Iterator[Action]:
        yield from self.action_factory.create_actions(
            store
        )  # allow create, read, search and count.
        upload_store_meta = store.get_meta()
        api_access = upload_store_meta.store_security.get_api_access()
        if api_access.create_filter is not EXCLUDE_ALL:
            yield action_for_upload_finish(upload_store_meta)
        after_delete_action = create_action_for_after_delete(store.get_meta())
        if after_delete_action:
            yield after_delete_action
        yield create_reaper_action(store.get_meta())

    def create_routes(self, store: StoreABC) -> Iterator[Route]:
        yield from self.action_factory.create_routes(store)


def action_for_upload_finish(
    upload_store_meta: StoreMeta,
):
    @action(triggers=WEB_POST)
    def upload_finish(
        upload_id: str, authorization: Optional[Authorization]
    ) -> Optional[ChunkFileHandle]:
        upload_store: StoreABC[ChunkUpload] = upload_store_meta.create_secured_store(
            authorization
        )
        upload = upload_store.read(upload_id)
        if upload:
            file_handle = upload.finish_upload(authorization)
            return file_handle

    return get_action(upload_finish)
