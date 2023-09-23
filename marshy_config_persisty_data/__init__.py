from marshy.factory.impl_marshaller_factory import register_impl
from marshy.marshaller_context import MarshallerContext
from marshy_config_servey import raise_non_ignored
from servey.servey_starlette.route_factory.route_factory_abc import RouteFactoryABC

from marshy_config_persisty_data.bytes_marshaller import BytesMarshaller
from persisty_data.data_store_route_factory import DataStoreRouteFactory


priority = 100


def configure(context: MarshallerContext):
    context.register_marshaller(BytesMarshaller(), bytes)
    try:
        register_impl(RouteFactoryABC, DataStoreRouteFactory, context)
    except ModuleNotFoundError as e:
        raise_non_ignored(e)
    configure_serverless(context)


def configure_serverless(context: MarshallerContext):
    try:
        from servey.servey_aws.serverless.yml_config.yml_config_abc import YmlConfigABC
        from persisty_data.s3_yml_config import S3YmlConfig

        register_impl(YmlConfigABC, S3YmlConfig, context)
    except ModuleNotFoundError as e:
        raise_non_ignored(e)
