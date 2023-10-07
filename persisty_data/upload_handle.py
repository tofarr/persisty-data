from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from persisty.result_set import result_set_dataclass_for

from persisty_data.finder.file_store_finder_abc import find_file_store_by_name
from persisty_data.upload_part import UploadPartResultSet


@dataclass
class UploadHandle:
    id: str
    store_name: str
    file_name: str
    content_type: Optional[str]
    expire_at: datetime

    @property
    def parts(self) -> UploadPartResultSet:
        file_store = find_file_store_by_name(self.store_name)
        return file_store.upload_part_search(self.id)

    @property
    def part_count(self) -> int:
        file_store = find_file_store_by_name(self.store_name)
        return file_store.upload_part_search(self.id)


UploadHandleResultSet = result_set_dataclass_for(UploadHandle)
