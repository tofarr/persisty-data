
from typing import Iterator

from persisty.servey.action_factory_abc import ActionFactoryABC, _StoreABC, ROUTE
from servey.action.action import Action


class ChunkUploadPartStoreActionFactory(ActionFactoryABC):
    def create_actions(self, store: _StoreABC) -> Iterator[Action]:
        # allow create, read and search.
        # Allow finalize
        pass

    def create_routes(self, store: _StoreABC) -> Iterator[ROUTE]:
        pass