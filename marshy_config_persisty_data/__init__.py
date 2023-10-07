from marshy.factory.impl_marshaller_factory import register_impl
from marshy.marshaller_context import MarshallerContext
from marshy_config_servey import raise_non_ignored
from servey.finder.action_finder_abc import ActionFinderABC
from servey.servey_starlette.route_factory.route_factory_abc import RouteFactoryABC

from marshy_config_persisty_data.bytes_marshaller import BytesMarshaller


priority = 100


def configure(context: MarshallerContext):
    from persisty_data.finder.module_file_store_finder import ModuleFileStoreFinder
    from persisty_data.finder.file_store_finder_abc import FileStoreFinderABC
    from persisty.finder.store_meta_finder_abc import StoreMetaFinderABC
    from persisty_data.finder.file_store_store_meta_finder import (
        FileStoreStoreMetaFinder,
    )
    from persisty_data.finder.file_store_action_finder import FileStoreActionFinder
    from persisty_data.finder.file_store_route_factory import FileStoreRouteFactory

    context.register_marshaller(BytesMarshaller(), bytes)
    register_impl(FileStoreFinderABC, ModuleFileStoreFinder, context)
    register_impl(StoreMetaFinderABC, FileStoreStoreMetaFinder, context)
    register_impl(ActionFinderABC, FileStoreActionFinder, context)
    register_impl(RouteFactoryABC, FileStoreRouteFactory, context)
    try:
        from persisty_data.finder.file_store_route_factory import FileStoreRouteFactory

        register_impl(RouteFactoryABC, FileStoreRouteFactory, context)
    except ModuleNotFoundError as e:
        raise_non_ignored(e)
    configure_serverless(context)


def configure_serverless(context: MarshallerContext):
    try:
        from servey.servey_aws.serverless.yml_config.yml_config_abc import YmlConfigABC
        from persisty_data.s3.s3_yml_config import S3YmlConfig

        register_impl(YmlConfigABC, S3YmlConfig, context)
    except ModuleNotFoundError as e:
        raise_non_ignored(e)
