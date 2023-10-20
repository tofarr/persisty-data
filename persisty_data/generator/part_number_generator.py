from dataclasses import dataclass

from persisty.attr.attr_filter import attr_eq
from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.store_meta import get_meta
from persisty.util import UNDEFINED


@dataclass
class PartNumberGenerator(AttrValueGeneratorABC):
    key_attr: str = "upload_id"

    def transform(self, value, item):
        if value is UNDEFINED:
            search_filter = attr_eq(self.key_attr, getattr(item, self.key_attr))
            store_meta = get_meta(item)
            value = store_meta.create_store().count(search_filter) + 1
        return value
