import hashlib
from dataclasses import dataclass, field
from typing import Iterator, Optional

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.finder.stored_finder_abc import find_stored_by_name
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr
from persisty.servey.action_factory import ActionFactory
from persisty.servey.action_factory_abc import ActionFactoryABC
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta
from servey.action.action import Action, action, get_action
from servey.security.authorization import Authorization
from servey.trigger.web_trigger import WEB_POST
from starlette.routing import Route

from persisty_data.v6.chunk.model.chunk_file_handle import ChunkFileHandle
from persisty_data.v6.chunk.model.chunk_upload import ChunkUpload


@dataclass
class ChunkUploadPartStoreActionFactory(ActionFactoryABC):
    action_factory: ActionFactoryABC = field(default_factory=ActionFactory)

    def create_actions(self, store: StoreABC) -> Iterator[Action]:
        yield from self.action_factory.create_actions(store)  # allow create, read and search.
        upload_store_meta = store.get_meta()
        api_access = upload_store_meta.store_security.get_api_access()
        if api_access.create_filter is not EXCLUDE_ALL:
            file_handle_store_meta = find_stored_by_name(upload_store_meta.name[:-7])
            chunk_store_meta = find_stored_by_name(file_handle_store_meta.name+"_chunk")
            download_url_pattern = "/data/" + file_handle_store_meta.name.replace('_', '-') + "/{key:path}"
            yield action_for_upload_finish(file_handle_store_meta, upload_store_meta, chunk_store_meta, download_url_pattern)

    def create_routes(self, store: StoreABC) -> Iterator[Route]:
        yield from self.action_factory.create_routes(store)


def action_for_upload_finish(
    file_handle_store_meta: StoreMeta,
    upload_store_meta: StoreMeta,
    chunk_store_meta: StoreMeta,
    download_url_pattern: str
):

    @action(triggers=WEB_POST)
    def upload_finish(upload_id: str, authorization: Optional[Authorization]) -> Optional[ChunkFileHandle]:
        upload_store: StoreABC[ChunkUpload] = upload_store_meta.create_secured_store(authorization)
        upload = upload_store.read(upload_id)
        if upload:
            search_filter = AttrFilter("upload_id", AttrFilterOp.eq, upload)
            search_order = SearchOrder((SearchOrderAttr("part_number"),))
            chunk_store = chunk_store_meta.create_store()
            chunks = chunk_store.search_all(search_filter, search_order)
            md5 = hashlib.md5()
            size_in_bytes = 0
            for chunk in chunks:
                size_in_bytes += len(chunk.data)
                md5.update(chunk.data)
            etag = md5.hexdigest()
            file_handle = file_handle_store_meta.get_stored_dataclass()(
                key=upload.item_key,
                upload_id=upload.id,
                content_type=upload.content_type,
                size_in_bytes=size_in_bytes,
                etag=etag,
                download_url=download_url_pattern.format(key=upload.item_key)
            )
            file_handle_store = file_handle_store_meta.create_store()
            file_handle = file_handle_store.create(file_handle)
            upload_store.delete(upload_id)
            return file_handle

    return get_action(upload_finish)
