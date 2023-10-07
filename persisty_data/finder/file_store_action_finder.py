from typing import Iterator

from servey.action.action import Action
from servey.finder.action_finder_abc import ActionFinderABC

from persisty_data.finder.file_store_finder_abc import find_file_stores


class FileStoreActionFinder(ActionFinderABC):
    def find_actions(self) -> Iterator[Action]:
        for file_store in find_file_stores():
            yield from file_store.get_actions()
