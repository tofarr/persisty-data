from dataclasses import dataclass, field
from typing import Iterator

import marshy
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

from persisty_data.v6.chunk.chunk import CHUNK_SIZE


@dataclass
class UploadPartActionFactory(ActionFactoryABC):
    action_factory: ActionFactoryABC = field(default_factory=ActionFactory)

    def create_actions(self, store: StoreABC) -> Iterator[Action]:
        # allow create, read, search, and count.
        yield from self.action_factory.create_actions(store)

    def create_routes(self, store: StoreABC) -> Iterator[Route]:
        yield from self.action_factory.create_routes(store)
        upload_part_store_meta = store.get_meta()
        store_access = store.get_meta().store_security.get_api_access()
        if store_access.create_filter is not EXCLUDE_ALL:
            authorizer = get_default_authorizer()
            yield create_route_for_part_upload(
                upload_part_store_meta, authorizer
            )


def create_route_for_part_upload(
    upload_part_store_meta: StoreMeta,
    authorizer: AuthorizerABC,
    chunk_size: int = CHUNK_SIZE,
):
    async def upload(request: Request) -> Response:
        part_id = request.path_params.get("part_id")
        token = request.headers.get("authorization")
        authorization = authorizer.authorize(token) if token else None
        upload_part_store = upload_part_store_meta.create_secured_store(authorization)
        upload_part = upload_part_store.read(part_id)
        upload_part.size_in_bytes = await upload_part.save_content(
            _bytes_iterator(request, chunk_size)
        )
        upload_part = upload_part_store.update(upload_part)
        return JSONResponse(status_code=200, content=marshy.dump(upload_part))

    return Route(
        "/data/" + upload_part_store_meta.name.replace("_", "-") + "/{part_id}",
        name=upload_part_store_meta.name + "_upload",
        endpoint=upload,
        methods=["POST", "PUT", "PATCH"],
    )


async def _bytes_iterator(request: Request, chunk_size: int):
    buffer = bytearray()
    async for data in request.stream():
        data_offset = 0
        while data_offset < len(data):
            if len(buffer) + len(data) - data_offset >= chunk_size:
                if buffer:
                    buffer.extend(data[data_offset : data_offset + chunk_size])
                    yield bytes(buffer)
                    buffer.clear()
                else:
                    yield data[data_offset : data_offset + chunk_size]
    if buffer:
        yield bytes(buffer)
