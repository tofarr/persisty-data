import dataclasses
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
from typing import Iterator, Optional, Tuple, Mapping

import marshy
from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.errors import PersistyError
from persisty.finder.stored_finder_abc import find_stored_by_name
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr
from persisty.servey.action_factory import ActionFactory
from persisty.servey.action_factory_abc import ActionFactoryABC
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta
from servey.action.action import Action
from servey.cache_control.cache_control_abc import CacheControlABC
from servey.security.authorizer.authorizer_abc import AuthorizerABC
from servey.security.authorizer.authorizer_factory_abc import get_default_authorizer
from starlette.datastructures import MutableHeaders
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route
from starlette.types import Scope, Receive, Send

from persisty_data.v6.chunk.model.chunk_file_handle import ChunkFileHandle


@dataclass
class ChunkUploadPartStoreActionFactory(ActionFactoryABC):
    action_factory: ActionFactoryABC = field(default_factory=ActionFactory)

    def create_actions(self, store: StoreABC) -> Iterator[Action]:
        yield from self.action_factory.create_actions(store)

    def create_routes(self, store: StoreABC) -> Iterator[Route]:
        yield from self.action_factory.create_routes(store)
        file_handle_store_meta = store.get_meta()
        api_access = file_handle_store_meta.store_security.get_api_access()
        if api_access.read_filter is not EXCLUDE_ALL:
            chunk_store_meta = find_stored_by_name(file_handle_store_meta.name + "_chunk")
            authorizer = get_default_authorizer()
            yield create_route_for_download(file_handle_store_meta, chunk_store_meta, authorizer)


def create_route_for_download(
    file_handle_store_meta: StoreMeta, chunk_store_meta: StoreMeta, authorizer: AuthorizerABC,
):
    async def download(request: Request) -> Response:
        key = request.path_params.get("key")
        token = request.headers.get("authorization")
        authorization = authorizer.authorize(token) if token else None
        file_handle_store = file_handle_store_meta.create_secured_store(authorization)
        file_handle = file_handle_store.read(key)
        if not file_handle:
            return Response(status_code=404)

        if request.method.lower() != "get":
            return Response(status_code=200, headers={
                "Content-Type": "",
                "ETag": "",
                "": ""
            })

        range = parse_ranges(request)
        chunk_store = chunk_store_meta.create_store()
        return chunk_response(request.method.lower(), request.headers, file_handle, file_handle_store.get_meta().cache_contro, chunk_store, range)

    return Route(
        "/data/" + file_handle_store_meta.name.replace('_', '-') + "/{key:path}",
        name=file_handle_store_meta.name + "_download",
        endpoint=download,
        methods=["GET", "HEAD", "OPTIONS"],
    )


def parse_ranges(request: Request) -> Optional[Tuple[int, int]]:
    range_header = (request.headers.get("range") or "").replace(" ", "").lower()
    if not range_header:
        return
    if not range_header.startswith("bytes="):
        raise PersistyError('invalid_range')
    try:
        range_header = range_header[6:]
        if ',' in range_header:
            raise PersistyError('multipart_range_not_supported')
        start, end = (int(n) for n in range_header.split('-'))
        return start, end
    except:
        raise PersistyError('invalid_range')



def chunk_response(
    method: str,
    request_headers: Mapping[str, str],
    file_handle: Optional[ChunkFileHandle],
    cache_control: CacheControlABC,
    chunk_store: StoreABC,
    range: Optional[Tuple[int, int]]
) -> Response:
    if not file_handle:
        return Response(status_code=404)
    cache_header = cache_control.get_cache_header(marshy.dump(file_handle))
    if cache_header.etag:
        cache_header = dataclasses.replace(cache_header, etag=file_handle.etag)

    http_headers = cache_header.get_http_headers()
    if file_handle.content_type:
        http_headers["content-type"] = file_handle.content_type
    http_headers["Accept-Ranges"] = "bytes"

    if_none_match = request_headers.get("If-None-Match")
    if_modified_since = request_headers.get("If-Modified-Since")
    if if_none_match and cache_header.etag:
        if cache_header.etag == if_none_match:
            return Response(status_code=304, headers=http_headers)
    elif if_modified_since and cache_header.updated_at:
        if_modified_since_date = parsedate_to_datetime(if_modified_since)
        if if_modified_since_date >= cache_header.updated_at:
            return Response(status_code=304, headers=http_headers)
    if method in ("head", "options"):
        http_headers["Content-Length"] = str(file_handle.size_in_bytes)
        return Response(status_code=200, headers=http_headers)
    if range:
        if range[1] > file_handle.size_in_bytes:
            return Response(status_code=416, headers=http_headers)
        http_headers["Content-Range"] = f"bytes {range[0]}-{range[1]}/{file_handle.size_in_bytes}"
        http_headers["Content-Length"] = str(range[1])
    else:
        http_headers["Content-Length"] = str(file_handle.size_in_bytes)
    response = _ChunkFileHandleResponse(
        status_code=200, headers=http_headers, chunk_store=chunk_store, file_handle=file_handle, range=range
    )
    return response


class _ChunkFileHandleResponse(Response):
    # pylint: disable=W0231
    # noinspection PyMissingConstructor
    def __init__(
        self, status_code, headers, chunk_store, file_handle, range
    ):
        self.status_code = status_code
        self._headers = MutableHeaders(headers=headers)
        self.chunk_store = chunk_store
        self.file_handle = file_handle
        self.range = range

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": self._headers.raw,
            }
        )
        search_filter = AttrFilter("upload_id", AttrFilterOp.eq, self.file_handle.upload_id)
        search_order = SearchOrder((SearchOrderAttr("sort_key"),))
        offset = self.range[0] if self.range else 0
        limit = ((self.range[1] + 1) if self.range else self.file_handle.size_in_bytes) - offset
        for chunk in self.chunk_store.search_all(search_filter, search_order):
            data = chunk.data
            if offset:
                if offset >= len(data):
                    offset -= len(data)
                    continue
                data = data[offset:]
                offset = 0
            if limit < len(data):
                data = data[:limit]
                await send(
                    {
                        "type": "http.response.body",
                        "body": data,
                        "more_body": False,
                    }
                )
                return
            limit -= len(data)
            await send(
                {
                    "type": "http.response.body",
                    "body": data,
                    "more_body": bool(limit),
                }
            )
