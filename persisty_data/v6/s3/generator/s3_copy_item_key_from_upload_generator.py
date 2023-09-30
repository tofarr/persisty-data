from dataclasses import dataclass

from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.util import UNDEFINED


@dataclass
class S3CopyItemKeyFromUploadGenerator(AttrValueGeneratorABC):

    def transform(self, value, item):
        if value is UNDEFINED:
            store = item.get_upload_store_meta().create_store()
            upload = store.read(item.upload_id)
            value = upload.item_key
        return value
