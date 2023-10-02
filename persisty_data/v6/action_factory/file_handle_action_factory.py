import dataclasses
from dataclasses import field, dataclass
from email.utils import parsedate_to_datetime
from typing import Iterator, Optional, Tuple, Mapping

import marshy
from persisty.errors import PersistyError
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.servey.action_factory import ActionFactory
from persisty.servey.action_factory_abc import ActionFactoryABC, _StoreABC, ROUTE
from persisty.store_meta import StoreMeta
from servey.action.action import Action
from servey.cache_control.cache_control_abc import CacheControlABC
from servey.security.authorizer.authorizer_abc import AuthorizerABC
from servey.security.authorizer.authorizer_factory_abc import get_default_authorizer
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route

from persisty_data.v6.action_factory.after_delete_action import (
    create_action_for_after_delete,
)
from persisty_data.v6.action_factory.reaper_action import create_reaper_action
from persisty_data.v6.model.file_handle import FileHandle


@dataclass
class FileHandleActionFactory(ActionFactoryABC):
    action_factory: ActionFactoryABC = field(default_factory=ActionFactory)

    def create_actions(self, store: _StoreABC) -> Iterator[Action]:
        yield from self.action_factory.create_actions(store)
        after_delete_action = create_action_for_after_delete(store.get_meta())
        if after_delete_action:
            yield after_delete_action
        yield create_reaper_action(store.get_meta())

    def create_routes(self, store: _StoreABC) -> Iterator[ROUTE]:
        yield from self.action_factory.create_routes(store)
        file_handle_store_meta = store.get_meta()
        api_access = file_handle_store_meta.store_security.get_api_access()
        if api_access.read_filter is not EXCLUDE_ALL:
            authorizer = get_default_authorizer()
            yield self.create_route_for_download(file_handle_store_meta, authorizer)

    def create_route_for_download(
        self,
        file_handle_store_meta: StoreMeta,
        authorizer: AuthorizerABC,
    ):
        async def download(request: Request) -> Response:
            key = request.path_params.get("key")
            token = request.headers.get("authorization")
            authorization = authorizer.authorize(token) if token else None
            file_handle_store = file_handle_store_meta.create_secured_store(
                authorization
            )
            file_handle = file_handle_store.read(key)
            if not file_handle:
                return Response(status_code=404)

            if request.method.lower() != "get":
                return Response(
                    status_code=200, headers={"Content-Type": "", "ETag": "", "": ""}
                )

            return file_handle_response(
                method=request.method.lower(),
                request_headers=request.headers,
                file_handle=file_handle,
                cache_control=file_handle_store.get_meta().cache_control,
                range=parse_ranges(request),
            )

        return Route(
            "/data/" + file_handle_store_meta.name.replace("_", "-") + "/{key:path}",
            name=file_handle_store_meta.name + "_download",
            endpoint=download,
            methods=["GET", "HEAD", "OPTIONS"],
        )


def parse_ranges(request: Request) -> Optional[Tuple[int, int]]:
    range_header = (request.headers.get("range") or "").replace(" ", "").lower()
    if not range_header:
        return
    if not range_header.startswith("bytes="):
        raise PersistyError("invalid_range")
    try:
        range_header = range_header[6:]
        if "," in range_header:
            raise PersistyError("multipart_range_not_supported")
        start, end = (int(n) for n in range_header.split("-"))
        return start, end
    except:
        raise PersistyError("invalid_range")


def file_handle_response(
    method: str,
    request_headers: Mapping[str, str],
    file_handle: FileHandle,
    cache_control: CacheControlABC,
    range: Optional[Tuple[int, int]],
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
        http_headers[
            "Content-Range"
        ] = f"bytes {range[0]}-{range[1]}/{file_handle.size_in_bytes}"
        http_headers["Content-Length"] = str(range[1])
    else:
        http_headers["Content-Length"] = str(file_handle.size_in_bytes)
    response = StreamingResponse(
        status_code=200,
        headers=http_headers,
        content=content_iterator(file_handle, range),
    )
    return response


def content_iterator(
    file_handle: FileHandle, range: Optional[Tuple[int, int]]
) -> Iterator[bytes]:
    to_read = range[1] if range else file_handle.size_in_bytes
    with file_handle.get_reader() as reader:
        if range:
            reader.seek(range[0])
        while to_read:
            data = reader.read()
            to_read -= len(data)
            yield data
