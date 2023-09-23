from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from servey.action.action import action


@dataclass
class DataHandle:
    store: str
    key: str
    updated_at: datetime
    etag: str
    content_type: Optional[str] = None

    @action
    @property
    def download_url(self):
        """Get the download url for this data handle."""
        # TODO: Batching
        data_store = find_data_store(self.store)
        return data_store.get_download_url(self.key)
