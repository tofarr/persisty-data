from datetime import datetime
from typing import ForwardRef, Optional
from uuid import UUID

from persisty.impl.dynamodb.partition_sort_index import PartitionSortIndex
from persisty.link.belongs_to import BelongsTo
from persisty.security.owned_store_security import OwnedStoreSecurity
from persisty.stored import stored
from persisty_data.has_url import HasUrl

from messager.store.message_state import MessageState


@stored(
    indexes=(
        PartitionSortIndex("author_id", "created_at"),
        PartitionSortIndex("message_state", "created_at"),
    ),
    store_security=OwnedStoreSecurity(subject_id_attr_name="author_id"),
)
class Message:
    """Item representing a message object"""

    id: UUID
    message_text: str
    message_state: Optional[MessageState] = MessageState.FEATURED
    created_at: datetime
    updated_at: datetime
    author_id: UUID
    author: ForwardRef("servey_main.models.user.User") = BelongsTo()
    message_image_url: Optional[str] = HasUrl()
