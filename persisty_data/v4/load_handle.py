from dataclasses import dataclass
from io import IOBase

from persisty_data.v4.file_handle import FileHandle


@dataclass
class LoadHandle:
    file_handle: FileHandle
    reader: IOBase
