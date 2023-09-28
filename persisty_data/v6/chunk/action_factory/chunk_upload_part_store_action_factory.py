from dataclasses import dataclass, field
from typing import Iterator, Callable
from uuid import uuid4

import marshy
from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.finder.stored_finder_abc import find_stored_by_name
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.servey.action_factory import ActionFactory
from persisty.servey.action_factory_abc import ActionFactoryABC
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta
from servey.action.action import Action
from servey.security.authorizer.authorizer_abc import AuthorizerABC
from servey.security.authorizer.authorizer_factory_abc import get_default_authorizer
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from starlette.routing import Route

from persisty_data.v5.upload_part import UploadPart
from persisty_data.v6.chunk.model.chunk import CHUNK_SIZE


@dataclass
class ChunkUploadPartStoreActionFactory(ActionFactoryABC):
    action_factory: ActionFactoryABC = field(default_factory=ActionFactory)

    def create_actions(self, store: StoreABC) -> Iterator[Action]:
        # allow create, read and search. Upload Part should have status. Empty and filled. should also
        yield from self.action_factory.create_actions(store)

    def create_routes(self, store: StoreABC) -> Iterator[Route]:
        yield from self.action_factory.create_routes(store)
        upload_part_store_meta = store.get_meta()
        store_access = store.get_meta().store_security.get_api_access()
        if store_access.create_filter is not EXCLUDE_ALL:
            chunk_store_meta = find_stored_by_name(upload_part_store_meta.name[:-11] + "chunk")
            authorizer = get_default_authorizer()
            yield create_route_for_part_upload(upload_part_store_meta, chunk_store_meta, authorizer)


def create_route_for_part_upload(
    upload_part_store_meta: StoreMeta, chunk_store_meta: StoreMeta, authorizer: AuthorizerABC, chunk_size: int = CHUNK_SIZE
):
    async def upload(request: Request) -> Response:
        part_id = request.path_params.get("part_id")
        token = request.headers.get("authorization")
        authorization = authorizer.authorize(token) if token else None
        upload_part_store = upload_part_store_meta.create_secured_store(authorization)
        upload_part = upload_part_store.read(part_id)
        chunk_type = chunk_store_meta.get_create_dataclass()
        chunk_store = chunk_store_meta.create_secured_store(authorization)
        chunk_store.delete_all(AttrFilter("part_id", AttrFilterOp.eq, part_id))
        async for chunk in _chunk_iterator(request, upload_part, chunk_type, chunk_size):
            chunk_store.create(chunk)
        upload_part.size_in_bytes = chunk_store
        upload_part = upload_part_store.update(upload_part)
        return JSONResponse(status_code=200, content=marshy.dump(upload_part))

    return Route(
        "/data/" + upload_part_store_meta.name.replace('_', '-') + "/{part_id}",
        name=upload_part_store_meta.name + "_upload",
        endpoint=upload,
        methods=["POST", "PUT", "PATCH"],
    )


async def _chunk_iterator(request: Request, upload_part: UploadPart, chunk_type: Callable, chunk_size: int):
    chunk_number = 0
    async for data in _bytes_iterator(request, chunk_size):
        chunk_number += 1
        chunk = chunk_type(
            id=uuid4(),
            item_key=upload_part,
            upload_id=upload_part.upload_id,
            part_id=str(upload_part.id),
            part_number=upload_part.part_number,
            chunk_number=chunk_number,
            sort_key=upload_part.part_number * 1024 * 1024 + chunk_number,
            data=data,
        )
        yield chunk


async def _bytes_iterator(request: Request, chunk_size: int):
    buffer = bytearray()
    async for data in request.stream():
        data_offset = 0
        while data_offset < len(data):
            if len(buffer) + len(data) - data_offset >= chunk_size:
                if buffer:
                    buffer.extend(data[data_offset:data_offset+chunk_size])
                    yield bytes(buffer)
                    buffer.clear()
                else:
                    yield data[data_offset:data_offset+chunk_size]
    if buffer:
        yield bytes(buffer)
