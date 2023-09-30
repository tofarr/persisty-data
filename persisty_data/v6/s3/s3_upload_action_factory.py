from typing import Iterator

from persisty.servey.action_factory_abc import ActionFactoryABC, _StoreABC, ROUTE
from servey.action.action import Action


class S3UploadActionFactory(ActionFactoryABC):
    def create_actions(self, store: _StoreABC) -> Iterator[Action]:
        # action for read, read_batch, search, count
        # action for create - calls create_multipart_upload and then save in standard db
        # action for finish - calls complete_multipart upload and then saves in standard db


    def create_routes(self, store: _StoreABC) -> Iterator[ROUTE]:
        pass