"""
Subscription used for background task for copying data from an external url to a chunk store
"""
from servey.action.action import get_action
from servey.security.access_control.allow_none import ALLOW_NONE
from servey.subscription.subscription import subscription

from persisty_data.v4.chunk.chunked_data_copy_action import chunked_data_copy_action
from persisty_data.v4.chunk.chunked_data_copy_event import ChunkedDataCopyEvent

chunked_data_copy_subscription = subscription(
    event_type=ChunkedDataCopyEvent,
    name="chunked_data_copy",
    access_control=ALLOW_NONE,
    action_subscribers=(get_action(chunked_data_copy_action),),
)
