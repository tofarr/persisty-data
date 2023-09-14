from persisty_data.data_store_abc import DataStoreABC


def default_data_store_factory(data_store: DataStoreABC):
    # check for s3 - we assume we are hosting otherwise
    data_store.