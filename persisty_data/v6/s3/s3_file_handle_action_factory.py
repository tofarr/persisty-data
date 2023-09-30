from typing import Iterator

from persisty.servey.action_factory_abc import ActionFactoryABC, _StoreABC, ROUTE
from servey.action.action import Action


class S3FileHandleActionFactory(ActionFactoryABC):
    def create_actions(self, store: _StoreABC) -> Iterator[Action]:
        # action for read, read_batch, search, count
        # read search
        # should presigned urls be regenerated on the fly, or later?

    def create_routes(self, store: _StoreABC) -> Iterator[ROUTE]:
        pass