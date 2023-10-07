from persisty_data.directory.directory_file_store import DirectoryFileStore
from persisty_data.file_store_meta import FileStoreMeta

message_image_file_store = DirectoryFileStore(meta=FileStoreMeta(name="message_image"))
