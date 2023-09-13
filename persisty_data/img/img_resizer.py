import io
from dataclasses import dataclass, field
from typing import FrozenSet, Optional, Dict

from PIL import Image, ImageChops
from persisty.errors import PersistyError
from servey.action.action import action
from servey.security.authorization import Authorization
from servey.trigger.web_trigger import WEB_GET

from persisty_data.data_store_factory_abc import DataStoreFactoryABC, find_data_store_factories
from persisty_data.mem_data_item import MemDataItem


def get_data_store_by_name() -> Dict[str, DataStoreFactoryABC]:
    data_stores = {
        f.get_meta().name: f for f in find_data_store_factories()
    }
    return data_stores


@dataclass
class ImgResizer:
    resized_store_name: str = "resized_image"
    store_factories: DataStoreFactoryABC = field(default_factory=lambda: {
        f.get_meta().name: f for f in find_data_store_factories()
    })
    max_width: int = 1024
    max_height: int = 768
    permitted_content_types: FrozenSet[str] = frozenset(("image/png",))

    def get_resized_image_key(self, store: str, key: str, width: int, height: int, content_type: str):
        if store == self.resized_store_name:
            raise PersistyError(f'invalid_store:{store}')
        resized_image_key = f"{store}/{width}_{height}_{content_type}/{key}"
        return resized_image_key

    @property
    def resized_store_factory(self):
        return self.store_factories[self.resized_store_name]

    def sanitize_content_type(self, content_type: Optional[str]):
        if content_type is None:
            content_type = next(iter(self.permitted_content_types))
            return content_type
        if content_type not in self.permitted_content_types:
            raise PersistyError(f"invalid_content_type:{content_type}")
        return content_type

    def get_resized_image_download_url(
        self,
        store_name: str,
        key: str,
        width: Optional[int] = None,
        height: Optional[int] = None,
        content_type: Optional[str] = None,
        authorization: Optional[Authorization] = None,
    ):
        width = width or self.max_width
        height = height or self.max_height
        content_type = self.sanitize_content_type(content_type)
        resized_image_key = self.get_resized_image_key(store_name, key, width, height, content_type)
        resized_store = self.resized_store_factory.create(authorization)
        item = resized_store.read(key)
        if not item:
            source_store = self.store_factories[store_name].create(authorization)
            item = source_store.read(key)
            if not item:
                return
            img = Image.open(item.get_data_reader())
            resized_img = create_resized_img(img, width, height)
            #resized_img = image.resize((width, height))

            with io.BytesIO() as output:
                resized_img.save(output, format=content_type[6:])
                value = output.getvalue()
            item = MemDataItem(value, resized_image_key, _content_type=content_type)
            item = resized_store.create(item)

        download_url = item.data_url
        return download_url


def create_resized_img(img: Image, width: int, height: int):
    scale = min(width / img.width, height / img.height)
    paste_width = round(img.width * scale)
    paste_height = round(img.height * scale)
    paste_x = round((width - paste_width) / 2)
    paste_y = round((height - paste_height) / 2)
    resized_img = img.resize((paste_width, paste_height))
    result_img = Image.new("RGBA", (width, height))
    result_img.paste(resized_img, (paste_x, paste_y))
    return result_img
