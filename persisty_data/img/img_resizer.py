from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict

from PIL import Image
from persisty.errors import PersistyError
from servey.security.authorization import Authorization

from persisty_data.file_store_abc import FileStoreABC
from persisty_data.finder.file_store_finder_abc import find_file_stores


class ImgType(Enum):
    PNG = "png"
    JPG = "jpg"
    JPEG = "jpeg"
    GIF = "gif"
    WEBP = "webp"


IMG_MIME_TYPE = tuple(f"image/{t.value}" for t in ImgType)


@dataclass
class ImgResizer:
    resized_store: FileStoreABC = field(
        default_factory=lambda: next(
            s for s in find_file_stores() if s.get_meta().name == "resized_image"
        )
    )
    data_stores: Dict[str, FileStoreABC] = field(
        default_factory=lambda: {f.get_meta().name: f for f in find_file_stores()}
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

    @staticmethod
    def get_img_type(content_type: Optional[str]):
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
            raise PersistyError("invalid_dimensions")
        img_type = self.get_img_type(content_type)
        resized_image_key = self.get_resized_image_key(
            store_name, key, width, height, img_type
        )
        resized_store = self.resized_store
        item = resized_store.file_read(resized_image_key)
        if not item:
            source_store = self.data_stores[store_name]
            source_store = source_store.get_meta().store_security.get_secured(
                source_store, authorization
            )
            item = source_store.file_read(key)
            if not item:
                return
            img = Image.open(item.get_data_reader())
            resized_img = create_resized_img(img, width, height)

            with resized_store.content_write(
                resized_image_key, f"image/{img_type.value}"
            ) as output:
                resized_img.save(output, format=img_type.value)
            item = resized_store.file_read(resized_image_key)

        return item.download_url


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
