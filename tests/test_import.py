import importlib
import pkgutil
from unittest import TestCase


class TestImport(TestCase):
    def test_import(self):
        import persisty_data

        _load_modules(persisty_data)
        import marshy_config_persisty_data

        _load_modules(marshy_config_persisty_data)
        import schemey_config_persisty_data

        _load_modules(schemey_config_persisty_data)


def _load_modules(module):
    paths = []
    paths.extend(module.__path__)
    module_infos = list(pkgutil.walk_packages(paths))
    for module_info in module_infos:
        sub_module_name = module.__name__ + "." + module_info.name
        sub_module = importlib.import_module(sub_module_name)
        # noinspection PyTypeChecker
        _load_modules(sub_module)
