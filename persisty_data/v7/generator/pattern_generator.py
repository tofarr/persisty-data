from dataclasses import dataclass

from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.store_meta import get_meta


@dataclass
class PatternGenerator(AttrValueGeneratorABC):
    key: str
    pattern: str

    def transform(self, value, item):
        store_meta = get_meta(item)
        attr_value = getattr(item, self.key)
        value = self.pattern.format(store_name=store_meta.name, value=attr_value)
        return value
