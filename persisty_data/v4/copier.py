from dataclasses import dataclass

from persisty.finder.stored_finder_abc import find_stored
from servey.action.action import action, get_action
from servey.subscription.subscription import subscription


@dataclass
class DataCopyEvent:
    source_url: str
    destination_store_name: str
    destination_key: str


@action
def data_copier_action(event: DataCopyEvent):
    try:
        store_meta = find_stored_by_name(event.destination_store_name)
        with open(event.source_url) as reader:
            store_meta.action_factory
    except Exception as e:
        print("D'oh")


"""

data_copier = subscription(
    event_type=DataCopyEvent,
    name="data_copier,
    access_control=ALLOW_NONE,
    action_subscribers=(get_action(data_copier_action),)
)

"""
