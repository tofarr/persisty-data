import importlib
import os
import pkgutil
from dataclasses import field, dataclass
from typing import Iterator

from servey.util import get_servey_main

from persisty_data.finder.file_store_finder_abc import FileStoreFinderABC, _FileStoreABC


@dataclass
class ModuleFileStoreFinder(FileStoreFinderABC):
    root_module_name: str = field(
        default_factory=lambda: f"{os.environ.get('PERSISTY_MAIN') or get_servey_main()}.store"
    )

    def find_file_stores(self) -> Iterator[_FileStoreABC]:
        module = importlib.import_module(self.root_module_name)
        # noinspection PyTypeChecker
        yield from find_in_module(module)


def find_in_module(module) -> Iterator[_FileStoreABC]:
    yield from get_from_module(module)
    module_infos = list(pkgutil.walk_packages(path=module.__path__))
    for module_info in module_infos:
        sub_module_name = module.__name__ + "." + module_info.name
        sub_module = importlib.import_module(sub_module_name)
        yield from get_from_module(sub_module)


def get_from_module(module) -> Iterator[_FileStoreABC]:
    from persisty_data.file_store_abc import FileStoreABC

    for value in list(module.__dict__.values()):
        if isinstance(value, FileStoreABC):
            yield value
