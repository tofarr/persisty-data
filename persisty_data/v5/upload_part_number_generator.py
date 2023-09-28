from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.store_meta import get_meta
from persisty.util import UNDEFINED


class UploadPartNumberGenerator(AttrValueGeneratorABC):
    def transform(self, value, item):
        if value in (UNDEFINED, None):
            store = get_meta(item).create_store()
            search_filter = AttrFilter("upload_id", AttrFilterOp.eq, item.upload_id)
            value = store.count(search_filter) + 1
        return value
