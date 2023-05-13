import importlib
import pkgutil
from unittest import TestCase


class TestImport(TestCase):
    """
    Not a great test, but at least make sure that everything imports without error.
    """

    def test_import(self):
        import persisty_data

        _load_submodules(persisty_data)
        import marshy_config_persisty_data

        _load_submodules(marshy_config_persisty_data)
        import schemey_config_persisty_data

        _load_submodules(schemey_config_persisty_data)


def _load_submodules(module):
    if not hasattr(module, "__path__"):
        return  # Module was not a package...
    paths = []
    paths.extend(module.__path__)
    module_infos = list(pkgutil.walk_packages(paths))
    for module_info in module_infos:
        sub_module_name = module.__name__ + "." + module_info.name
        sub_module = importlib.import_module(sub_module_name)
        # noinspection PyTypeChecker
        _load_submodules(sub_module)
