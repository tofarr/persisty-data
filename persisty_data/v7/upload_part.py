from dataclasses import dataclass
from uuid import UUID

from persisty.result_set import result_set_dataclass_for


@dataclass
class UploadPart:
    id: UUID
    upload_id: UUID
    part_number: int
    upload_url: str


UploadPartResultSet = result_set_dataclass_for(UploadPart)
