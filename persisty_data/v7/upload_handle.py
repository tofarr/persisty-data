from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from uuid import UUID

from persisty.result_set import result_set_dataclass_for
from servey.action.action import action

from persisty_data.v7.finder.file_store_finder_abc import find_file_store_by_name

UploadPartResultSet = result_set_dataclass_for(str)


@dataclass
class UploadHandle:
    id: UUID
    store_name: str
    file_name: str
    content_type: Optional[str]
    expire_at: datetime

    @action
    def parts(self) -> UploadPartResultSet:
        file_store = find_file_store_by_name(self.store_name)
        return file_store.upload_part_search(self.id)

    @action
    def part_count(self):
        file_store = find_file_store_by_name(self.store_name)
        return file_store.upload_part_search(self.id)


UploadHandleResultSet = result_set_dataclass_for(UploadHandle)
