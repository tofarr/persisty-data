from typing import Iterator

from servey.servey_starlette.route_factory.route_factory_abc import RouteFactoryABC
from starlette.routing import Route

from persisty_data.v7.finder.file_store_finder_abc import find_file_stores


class FileStoreRouteFactory(RouteFactoryABC):
    def create_routes(self) -> Iterator[Route]:
        for data_store in find_file_stores():
            yield data_store.get_routes()
