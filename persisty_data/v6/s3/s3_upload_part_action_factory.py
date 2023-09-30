from typing import Iterator

from persisty.servey.action_factory_abc import ActionFactoryABC, _StoreABC, ROUTE
from servey.action.action import Action

SHOULD BE UNNEEDED - USE THE STANDARD PERSISTY ACTION FACTORY INSTEAD. (No routes)
class S3UploadPartActionFactory(ActionFactoryABC):
    def create_actions(self, store: StoreABC) -> Iterator[Action]:
        # standard: allow read, read_batch, search, and count.
        # action for create - generate presigned_url and store in standard db -
        # This should be unneeded

    def create_routes(self, store: StoreABC) -> Iterator[ROUTE]:
        pass