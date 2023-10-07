from dataclasses import dataclass
from typing import Optional

from marshy.types import ExternalItemType
from persisty_data.has_url import HasUrl


@dataclass
class HasImg(HasUrl):
    resized_img_url: Optional[str] = "/resized-img"

    def update_json_schema(self, json_schema: ExternalItemType):
        key_attr_schema = json_schema.get("properties").get(self.key_attr_name)
        file_store = self.get_linked_file_store()
        schema = file_store.get_json_schema()
        key_attr_schema["persistyImgStore"] = schema
