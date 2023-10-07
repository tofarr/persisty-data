from dataclasses import dataclass

from persisty.result_set import result_set_dataclass_for


@dataclass
class UploadPart:
    id: str
    upload_id: str
    part_number: int
    upload_url: str


UploadPartResultSet = result_set_dataclass_for(UploadPart)
