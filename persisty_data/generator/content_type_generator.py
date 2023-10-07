from dataclasses import dataclass
import mimetypes

from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.util import UNDEFINED


@dataclass
class ContentTypeGenerator(AttrValueGeneratorABC):
    key: str = "key"

    def transform(self, value, item):
        if value is UNDEFINED:
            key = getattr(item, self.key)
            value = mimetypes.guess_type(key)[0]
        return value
