from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Set

from servey.trigger.web_trigger import WebTriggerMethod

from persisty_data.form_field import FormField


@dataclass
class UploadForm:
    """Configuration for a presigned upload"""

    target_url: str
    max_file_size: int
    method: WebTriggerMethod = WebTriggerMethod.POST
    pre_populated_fields: Optional[List[FormField]] = None
    file_param: str = "file"
    expire_at: Optional[datetime] = None
    content_types: Optional[Set[str]] = None
    permit_ranges: bool = False
    permit_numbered_parts: bool = False


# 2 actions
# begin upload
# do upload
# complete_upload
# abort upload
