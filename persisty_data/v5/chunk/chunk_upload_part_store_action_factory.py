from typing import Iterator

from persisty.servey.action_factory_abc import ActionFactoryABC, _StoreABC, ROUTE
from servey.action.action import Action


class ChunkUploadPartStoreActionFactory(ActionFactoryABC):
    def create_actions(self, store: _StoreABC) -> Iterator[Action]:
        # allow create, read and search. Upload Part should have status. Empty and filled. should also
        # have an upload method.
        pass

    def create_routes(self, store: _StoreABC) -> Iterator[ROUTE]:
        # create the upload route here
        pass