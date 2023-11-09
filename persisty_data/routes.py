import dataclasses
from email.utils import parsedate_to_datetime
from typing import Mapping, Optional, Tuple, Iterator

import marshy
from persisty.errors import PersistyError
from servey.security.authorizer.authorizer_abc import AuthorizerABC
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route

from persisty_data.file_handle import FileHandle
from persisty_data.file_store_abc import FileStoreABC

CHUNK_SIZE = 64 * 1024


def create_route_for_part_upload(
    file_store: FileStoreABC,
    authorizer: AuthorizerABC,
):
    async def upload(request: Request) -> Response:
        part_id = request.path_params.get("part_id")

        token = request.headers.get("authorization")
        token = token[7:] if token and token.lower().startswith("bearer ") else None
        authorization = authorizer.authorize(token) if token else None

        secured_store = file_store.get_meta().store_security.get_secured(
            file_store, authorization
        )
        with secured_store.upload_write(part_id) as writer:
            async for data in request.stream():
                writer.write(data)
            writer.flush()
        return Response(status_code=200)

    name = file_store.get_meta().name
    return Route(
        "/data/" + name.replace("_", "-") + "/{part_id}",
        name=name + "_upload",
        endpoint=upload,
        methods=["POST", "PUT", "PATCH"],
    )


def file_handle_response(
    method: str,
    request_headers: Mapping[str, str],
    file_store: FileStoreABC,
    file_handle: FileHandle,
    byte_range: Optional[Tuple[int, int]],
) -> Response:
    if not file_handle:
        return Response(status_code=404)
    cache_header = file_store.get_meta().cache_control.get_cache_header(
        marshy.dump(file_handle)
    )
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
    if byte_range:
        if byte_range[1] > file_handle.size_in_bytes:
            return Response(status_code=416, headers=http_headers)
        http_headers[
            "Content-Range"
        ] = f"bytes {byte_range[0]}-{byte_range[1]}/{file_handle.size_in_bytes}"
        http_headers["Content-Length"] = str(byte_range[1])
    else:
        http_headers["Content-Length"] = str(file_handle.size_in_bytes)
    response = StreamingResponse(
        status_code=200,
        headers=http_headers,
        content=content_iterator(file_store, file_handle.file_name, byte_range),
    )
    return response


def content_iterator(
    file_store: FileStoreABC, file_name: str, byte_range: Optional[Tuple[int, int]]
) -> Iterator[bytes]:
    to_read = byte_range[1] if byte_range else None
    with file_store.content_read(file_name) as reader:
        if byte_range:
            reader.seek(byte_range[0])
        while True:
            data = reader.read(CHUNK_SIZE)
            if not data:
                return
            if to_read is not None:
                if to_read > len(data):
                    to_read -= len(data)
                    yield data
                else:
                    data = data[:to_read]
                    yield data
                    return
            else:
                yield data


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
    except ValueError as exc:
        raise PersistyError("invalid_range") from exc


def create_route_for_download(
    file_store: FileStoreABC,
    authorizer: AuthorizerABC,
):
    async def download(request: Request) -> Response:
        key = request.path_params.get("key")
        token = request.headers.get("authorization")
        token = token[7:] if token and token.lower().startswith("bearer ") else None
        authorization = authorizer.authorize(token) if token else None
        secured_file_store = file_store.get_meta().store_security.get_secured(
            file_store, authorization
        )
        file_handle = secured_file_store.file_read(key)
        if not file_handle:
            return Response(status_code=404)

        if request.method.lower() != "get":
            return Response(
                status_code=200, headers={"Content-Type": "", "ETag": "", "": ""}
            )

        return file_handle_response(
            method=request.method.lower(),
            request_headers=request.headers,
            file_store=file_store,
            file_handle=file_handle,
            byte_range=parse_ranges(request),
        )

    store_name = file_store.get_meta().name.replace("_", "-")
    return Route(
        "/data/" + store_name + "/{key:path}",
        name=store_name + "_download",
        endpoint=download,
        methods=["GET", "HEAD", "OPTIONS"],
    )
