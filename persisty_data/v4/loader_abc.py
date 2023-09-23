from abc import abstractmethod, ABC
from io import IOBase
from typing import Optional, Callable

from persisty.servey.action_factory_abc import ROUTE
from persisty.store_meta import StoreMeta
from servey.action.action import Action

from persisty_data.v4.file_handle import FileHandle
from persisty_data.v4.load_handle import LoadHandle


class LoaderABC(ABC):

    @abstractmethod
    def load_from_store(self, source_key: str) -> Optional[LoadHandle]:
        """ Read data to the key given in the current store """

    @abstractmethod
    def copy_from_store(self, source_key: str, destination: IOBase):
        """ Read data to the key given in the current store """

    @abstractmethod
    def copy_to_store(self, source: Callable[[], IOBase], destination_key: str):
        """ Write data from the source given to the store under the key given """
