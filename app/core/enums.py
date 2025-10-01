from enum import Enum


class StreamState(str, Enum):
    running = "running"
    error = "error"
