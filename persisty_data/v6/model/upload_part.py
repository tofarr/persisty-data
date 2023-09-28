from datetime import datetime
from typing import Type, Optional, AsyncIterator
from uuid import UUID

from persisty.attr.attr import Attr
from persisty.attr.generator.default_value_generator import DefaultValueGenerator
from persisty.security.owned_store_security import OwnedStoreSecurity
from persisty.security.store_access import NO_UPDATES
from persisty.security.store_security import StoreSecurity
from persisty.security.store_security_abc import StoreSecurityABC
from persisty.servey.action_factory_abc import ActionFactoryABC
from persisty.stored import stored

from persisty_data.v6.generator.part_number_generator import PartNumberGenerator
from persisty_data.v6.generator.pattern_generator import PatternGenerator


class UploadPart:
    """
    Creatable but not updatable or deletable
    """

    id: UUID
    subject_id: str
    upload_id: str
    part_number: int = Attr(create_generator=PartNumberGenerator("upload_id"))
    size_in_bytes: int = Attr(
        creatable=False, create_generator=DefaultValueGenerator(0)
    )
    upload_url: str = Attr(
        creatable=False,
        create_generator=PatternGenerator(
            "id", "/data/{store_name}-upload-part/{value}"
        ),
    )
    created_at: datetime
    updated_at: datetime

    async def save_content(self, content: AsyncIterator[bytes]):
        """Save data for this part"""


def create_stored_upload_part_type(
    store_name: str,
    base_type: Type[UploadPart] = UploadPart,
    store_security: Optional[StoreSecurityABC] = None,
    action_factory: Optional[ActionFactoryABC] = None,
):
    upload_type = type(store_name.title().replace("_", "") + "Upload", (base_type,), {})
    if not store_security:
        # Custom security which does not allow you to specify
        store_security = OwnedStoreSecurity(
            store_security=StoreSecurity(
                default_access=NO_UPDATES, api_access=NO_UPDATES
            ),
            require_ownership_for_read=True,
        )
    if not action_factory:
        from persisty_data.v6.action_factory.upload_part_action_factory import UploadPartActionFactory
        action_factory = UploadPartActionFactory()
    stored_type = stored(
        upload_type, store_security=store_security, action_factory=action_factory
    )
    return stored_type
