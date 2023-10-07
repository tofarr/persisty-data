from dataclasses import dataclass
from datetime import datetime

from dateutil.relativedelta import relativedelta
from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.util import UNDEFINED


@dataclass
class FutureTimestampGenerator(AttrValueGeneratorABC):
    expire_in: int = 3600

    def transform(self, value, item):
        if item is UNDEFINED:
            value = datetime.now() + relativedelta(seconds=self.expire_in)
        return value
