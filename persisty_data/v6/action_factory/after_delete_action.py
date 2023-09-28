from persisty.store_meta import StoreMeta
from persisty.trigger.after_delete_trigger import AfterDeleteTrigger
from servey.action.action import get_action, action


def create_action_for_after_delete(store_meta: StoreMeta):
    if not getattr(store_meta.get_stored_dataclass(), "after_delete", None):
        return

    @action(
        name=store_meta.name+"_after_delete",
        triggers=AfterDeleteTrigger(store_meta.name)
    )
    def after_delete(item: store_meta.get_stored_dataclass()) -> bool:
        item.after_delete()
        return True

    return get_action(after_delete)
