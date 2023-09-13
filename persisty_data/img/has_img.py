from dataclasses import dataclass
from typing import Optional

from marshy.types import ExternalItemType
from persisty_data.has_url import HasUrl


@dataclass
class HasImg(HasUrl):
    resized_img_url: Optional[str] = '/resized-img'

    def update_json_schema(self, json_schema: ExternalItemType):
        key_attr_schema = json_schema.get("properties").get(self.key_attr_name)
        factory = self.get_linked_data_store_factory()
        schema = factory.get_json_schema()
        schema["resizedImgUrl"] = self.resized_img_url
        key_attr_schema["persistyImgStore"] = schema
