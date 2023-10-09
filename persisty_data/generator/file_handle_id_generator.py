from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.util import UNDEFINED


class FileHandleIdGenerator(AttrValueGeneratorABC):
    def transform(self, value, item):
        if value is UNDEFINED:
            value = f"{item.store_name}/{item.file_name}"
        return value
