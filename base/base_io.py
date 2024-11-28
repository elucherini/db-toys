from pathlib import Path
from typing import Any, BinaryIO


class BaseIOManager:
    """
    Handling IO
    """

    path: Path
    file: BinaryIO | None

    def __init__(self, path: str):
        self.path = Path(path)
        self.file = None

    def read(self, offset: Any) -> Any:
        pass

    def append(self, entry: Any) -> None:
        pass

    def open(self):
        if self.file is None:
            self.file = open(self.path, "a+b")

    def close(self):
        if self.file:
            self.file.close()
            self.file = None

    def is_open(self) -> bool:
        return self.file is not None
