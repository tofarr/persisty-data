from dataclasses import dataclass

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.store_meta import get_meta, StoreMeta
from persisty.util import UNDEFINED


@dataclass
class ChunkUploadPartStreamIdGenerator(AttrValueGeneratorABC):
    chunk_upload_store_meta: StoreMeta

    def transform(self, value, item):
        if value in (UNDEFINED, None):
            store = get_meta(item).create_store()
            upload = store.read(str(item.upload_id))
            value = upload.stream_id
        return value
