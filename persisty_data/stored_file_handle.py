from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.result import result_dataclass_for
from persisty.result_set import result_set_dataclass_for
from persisty.search_filter.search_filter_factory import search_filter_dataclass_for
from persisty.search_order.search_order_factory import search_order_dataclass_for
from persisty.store_meta import get_meta
from persisty.stored import stored

from persisty_data.file_handle import FileHandle

StoredFileHandle = stored(FileHandle, key_config=AttrKeyConfig("file_name"))
FileHandleSearchFilter = search_filter_dataclass_for(get_meta(StoredFileHandle))
FileHandleSearchOrder = search_order_dataclass_for(get_meta(StoredFileHandle))
FileHandleResult = result_dataclass_for(StoredFileHandle)
FileHandleResultSet = result_set_dataclass_for(FileHandleResult)
