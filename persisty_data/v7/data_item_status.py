from enum import Enum


class DataItemStatus(Enum):
    EMPTY = "empty"
    WORKING = "working"
    READY = "ready"
    ERROR = "error"
