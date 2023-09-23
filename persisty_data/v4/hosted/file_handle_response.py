import dataclasses
from email.utils import parsedate_to_datetime
from typing import Mapping

import marshy
from servey.cache_control.cache_control_abc import CacheControlABC
from starlette.datastructures import MutableHeaders
from starlette.responses import Response
from starlette.types import Scope, Receive, Send

from persisty_data.v4.file_handle import FileHandle
from persisty_data.v4.file_handle_status import FileHandleStatus


class FileHandleResponse(Response):
    # pylint: disable=W0231
    # noinspection PyMissingConstructor
    def __init__(
        self, status_code, headers, file_handle: FileHandle, buffer_size: int = 64 * 1024
    ):
        self.status_code = status_code
        self._headers = MutableHeaders(headers=headers)
        self.file_handle = file_handle
        self.buffer_size = buffer_size

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": self._headers.raw,
            }
        )
        with self.file_handle.open_for_read() as reader:
            more_body = True
            while more_body:
                buffer = reader.read(self.buffer_size)
                more_body = len(buffer) == self.buffer_size
                await send(
                    {
                        "type": "http.response.body",
                        "body": buffer,
                        "more_body": more_body,
                    }
                )


def file_handle_response(
    request_headers: Mapping[str, str],
    file_handle: FileHandle,
    cache_control: CacheControlABC,
) -> Response:
    if not file_handle or file_handle.status != FileHandleStatus.READY:
        return Response(status_code=404)
    cache_header = cache_control.get_cache_header(marshy.dump(file_handle))
    if cache_header.etag:
        cache_header = dataclasses.replace(cache_header, etag=file_handle.etag)

    http_headers = cache_header.get_http_headers()
    if file_handle.content_type:
        http_headers["content-type"] = file_handle.content_type

    if_none_match = request_headers.get("If-None-Match")
    if_modified_since = request_headers.get("If-Modified-Since")
    if if_none_match and cache_header.etag:
        if cache_header.etag == if_none_match:
            return Response(status_code=304, headers=http_headers)
    elif if_modified_since and cache_header.updated_at:
        if_modified_since_date = parsedate_to_datetime(if_modified_since)
        if if_modified_since_date >= cache_header.updated_at:
            return Response(status_code=304, headers=http_headers)
    http_headers["content-length"] = str(file_handle.size_in_bytes)
    response = FileHandleResponse(
        status_code=200, headers=http_headers, file_handle=file_handle
    )
    return response
