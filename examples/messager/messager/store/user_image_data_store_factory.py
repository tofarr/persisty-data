from persisty_data.owned_data_store_factory import OwnedDataStoreFactory

from messager.store import user_image_data_store

user_image_data_store_factory = OwnedDataStoreFactory(
    user_image_data_store.create_default_factory()
)
