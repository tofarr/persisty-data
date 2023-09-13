import io
from dataclasses import dataclass, field
from typing import FrozenSet, Optional, Dict

from PIL import Image
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
    cache_store_name: str = "resized_image"
    store_factories: DataStoreFactoryABC = field(default_factory=lambda: {
        f.get_meta().name: f for f in find_data_store_factories()
    })
    max_width: int = 1024
    max_height: int = 768
    permitted_content_types: FrozenSet[str] = frozenset("img/png")

    def get_resized_image_key(self, store: str, key: str, width: int, height: int, content_type: str):
        if store == self.cache_store_name:
            raise PersistyError(f'invalid_store:{store}')
        resized_image_key = f"{store}/{width}_{height}_{content_type}/{key}"
        return resized_image_key

    @property
    def cached_store_factory(self):
        return self.store_factories[self.cache_store_name]

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
        cached_store_factory = self.cached_store_factory
        download_url = cached_store_factory.get_download_url(
            resized_image_key, authorization
        )
        if download_url:
            return download_url

        source_store = self.store_factories[store_name].create(authorization)
        item = source_store.read(key)
        if not item:
            return
        cache_store = cached_store_factory.create(authorization)
        image = Image.open(item.get_data_reader())
        resized_image = image.resize((width, height))

        with io.BytesIO() as output:
            resized_image.save(output, format=content_type[6:])
            item = MemDataItem(output, resized_image_key)
            cache_store.create(item)

        download_url = self.cache_store_factory.get_download_url(
            resized_image_key, authorization
        )
        return download_url