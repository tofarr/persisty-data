from dataclasses import dataclass

from persisty.result import Result
from persisty.result_set import ResultSet, result_set_dataclass_for

from persisty_data.v5.upload import Upload
from persisty_data.v5.upload_part import UploadPart

UploadPartResultSet = result_set_dataclass_for(UploadPart)


@dataclass
class BeginUploadResult:
    upload: Upload
    initial_parts: UploadPartResultSet
