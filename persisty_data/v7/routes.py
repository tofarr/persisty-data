from persisty.store_meta import StoreMeta
from servey.security.authorizer.authorizer_abc import AuthorizerABC
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from persisty_data.v7.file_store_abc import FileStoreABC
from persisty_data.v7.persisty.persisty_file_store import PersistyFileStore


def create_route_for_part_upload(
    file_store: FileStoreABC,
    authorizer: AuthorizerABC,
    chunk_size: int = CHUNK_SIZE,
):
    async def upload(request: Request) -> Response:
        part_id = request.path_params.get("part_id")
        token = request.headers.get("authorization")
        authorization = authorizer.authorize(token) if token else None

        file_store.content_write()


        upload_part_store = upload_part_store_meta.create_secured_store(authorization)
        upload_part = upload_part_store.read(part_id)
        upload_part.size_in_bytes = await upload_part.save_content(
            _bytes_iterator(request, chunk_size)
        )
        upload_part = upload_part_store.update(upload_part)
        return JSONResponse(status_code=200, content=marshy.dump(upload_part))

    return Route(
        "/data/" + file_store.get_meta().name.replace("_", "-") + "/{part_id}",
        name=upload_part_store_meta.name + "_upload",
        endpoint=upload,
        methods=["POST", "PUT", "PATCH"],
    )