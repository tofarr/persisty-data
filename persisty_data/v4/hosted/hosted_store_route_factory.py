import marshy
from persisty.store_meta import StoreMeta
from servey.security.authorizer.authorizer_abc import AuthorizerABC
from starlette.datastructures import UploadFile
from starlette.routing import Route
from starlette.requests import Request
from starlette.responses import Response, JSONResponse

from persisty_data.v4.file_handle import FileHandle
from persisty_data.v4.hosted.file_handle_response import file_handle_response


def create_route_for_download(
    download_path: str, store_meta: StoreMeta, authorizer: AuthorizerABC
):
    def download(request: Request) -> Response:
        key = request.path_params.get("key")
        token = request.path_params.get("token")
        authorization = authorizer.authorize(token) if token else None
        store = store_meta.create_secured_store(authorization)
        store_access = store.get_meta().store_security.get_potential_access()
        if not store_access.readable:
            return Response(status_code=404)
        file_handle: FileHandle = store.read(key)
        return file_handle_response(
            request_headers=request.headers,
            file_handle=file_handle,
            cache_control=store_meta.cache_control,
        )

    path = download_path.replace("{key}", "{key:path}")
    return Route(
        path,
        name=store_meta.name + "_public_download",
        endpoint=download,
        methods=["GET"],
    )


def create_route_for_upload(
    upload_path: str, store_meta: StoreMeta, authorizer: AuthorizerABC
):
    async def upload(request: Request) -> Response:
        form = await request.form()
        token = form.get("token")
        authorization = authorizer.authorize(token) if token else None
        operation, key = next(
            s.split("upload:")[-1]
            for s in authorization.scopes
            if s.startswith("upload:")
        )
        store = store_meta.create_secured_store(authorization)
        form_file: UploadFile = form["file"]
        file_handle_class = store.get_meta().get_create_dataclass()
        file_handle = file_handle_class(
            key=key, handle=form_file, content_type=form_file.content_type
        )
        if operation == "create":
            file_handle = store.create(file_handle)
        elif operation == "update":
            file_handle = store.update(file_handle)
        return JSONResponse(status_code=200, content=marshy.dump(file_handle))

    return Route(
        upload_path,
        name=store_meta.name + "_upload",
        endpoint=upload,
        methods=["POST", "PUT", "PATCH"],
    )
