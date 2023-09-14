import io
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict

from PIL import Image
from persisty.errors import PersistyError
from servey.security.authorization import Authorization

from persisty_data.data_store_abc import find_data_stores, DataStoreABC
from persisty_data.data_store_factory_abc import (
    DataStoreFactoryABC,
    find_data_store_factories,
)
from persisty_data.mem_data_item import MemDataItem


def get_data_store_by_name() -> Dict[str, DataStoreFactoryABC]:
    data_stores = {f.get_meta().name: f for f in find_data_store_factories()}
    return data_stores


class ImgType(Enum):
    PNG = "png"
    JPG = "jpg"
    JPEG = "jpeg"
    GIF = "gif"
    WEBP = "webp"


@dataclass
class ImgResizer:
    resized_store: DataStoreABC = field(
        default_factory=lambda: next(
            s for s in find_data_stores() if s.get_meta().name == "resized_image"
        )
    )
    store_factories: Dict[str, DataStoreFactoryABC] = field(
        default_factory=lambda: {
            f.get_meta().name: f for f in find_data_store_factories()
        }
    )
    max_width: int = 1024
    max_height: int = 768

    def get_resized_image_key(
        self, store: str, key: str, width: int, height: int, img_type: ImgType
    ):
        if store == self.resized_store.get_meta().name:
            return key
        resized_image_key = f"{store}/{width}_{height}/{key}.{img_type.value}"
        return resized_image_key

    def get_img_type(self, content_type: Optional[str]):
        if content_type is None:
            return ImgType.PNG
        img_type = content_type[6:]
        img_type = ImgType(img_type)
        return img_type

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
        if width > self.max_width or height > self.max_height:
            raise PersistyError('invalid_dimensions')
        img_type = self.get_img_type(content_type)
        resized_image_key = self.get_resized_image_key(
            store_name, key, width, height, img_type
        )
        resized_store = self.resized_store
        item = resized_store.read(resized_image_key)
        if not item:
            source_store = self.store_factories[store_name].create(authorization)
            item = source_store.read(key)
            if not item:
                return
            img = Image.open(item.get_data_reader())
            resized_img = create_resized_img(img, width, height)

            with io.BytesIO() as output:
                resized_img.save(output, format=img_type.value)
                value = output.getvalue()
            item = MemDataItem(
                value, resized_image_key, _content_type=f"image/{img_type.value}"
            )
            resized_store.create(item)

        resized_factory = self.store_factories[resized_store.get_meta().name]
        download_url = resized_factory.get_download_url(
            resized_image_key, authorization
        )
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
