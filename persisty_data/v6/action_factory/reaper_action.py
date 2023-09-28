from datetime import datetime

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.store_meta import StoreMeta
from servey.action.action import action, get_action
from servey.trigger.fixed_rate_trigger import FixedRateTrigger


def create_reaper_action(store_meta: StoreMeta, interval: int = 3600):

    @action(
        name=store_meta.name+"_reaper",
        triggers=FixedRateTrigger(interval)
    )
    def reaper():
        search_filter = AttrFilter('expire_at', AttrFilterOp.lt, datetime.now())
        store_meta.create_store().delete_all(search_filter)

    return get_action(reaper)
