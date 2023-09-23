from enum import Enum


class FileHandleStatus(Enum):
    PROCESSSING = "processing"
    ERROR = "error"
    READY = "ready"
