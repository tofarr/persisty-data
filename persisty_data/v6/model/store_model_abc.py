from abc import ABC

from persisty.finder.stored_finder_abc import find_stored_by_name


class StoreModelABC(ABC):

    @classmethod
    def get_file_handle_store_meta(cls):
        file_handle_store_meta = getattr(cls, "__file_handle_store_meta__", None)
        if not file_handle_store_meta:
            file_handle_store_meta = find_stored_by_name(cls.base_store_name())
        return file_handle_store_meta

    @classmethod
    def get_upload_store_meta(cls):
        upload_part_store_meta = getattr(cls, "__upload_store_meta__", None)
        if not upload_part_store_meta:
            upload_part_store_meta = find_stored_by_name(cls.base_store_name() + "_upload")
        return upload_part_store_meta

    @classmethod
    def get_upload_part_store_meta(cls):
        upload_part_store_meta = getattr(cls, "__upload_part_store_meta__", None)
        if not upload_part_store_meta:
            upload_part_store_meta = find_stored_by_name(cls.base_store_name() + "_upload_part")
        return upload_part_store_meta

    @classmethod
    def base_store_name(cls):
        raise NotImplementedError()
