from persisty_data.directory.directory_file_store import DirectoryFileStore
from persisty_data.persisty.persisty_file_store import PersistyFileStore
from persisty_data.file_store_meta import FileStoreMeta
from persisty_data.img.img_resizer import IMG_MIME_TYPE


"""
message_image_file_store = DirectoryFileStore(
    meta=FileStoreMeta(
        name="message_image",
        permitted_content_types=IMG_MIME_TYPE
    )
)

img_resize_file_store = DirectoryFileStore(
    meta=FileStoreMeta(
        name="resized_image",
        permitted_content_types=IMG_MIME_TYPE
    )
)
"""

message_image_file_store = PersistyFileStore(
    meta=FileStoreMeta(
        name="message_image",
        permitted_content_types=IMG_MIME_TYPE
    )
)

img_resize_file_store = PersistyFileStore(
    meta=FileStoreMeta(
        name="resized_image",
        permitted_content_types=IMG_MIME_TYPE
    )
)

