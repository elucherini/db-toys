from pathlib import Path
from typing import Dict

from base.base_io import BaseIOManager


class RandomAccessLogManager(BaseIOManager):
    """
    Random Access-like IO that reads a line at a given offset and appends to the file.
    """
    def __init__(self, path):
        super().__init__(f"{path}/log.txt")

    def read(self, offset: int) -> Dict[str, str]:
        self.file.seek(offset)
        line = self.file.readline()
        if not line:
            raise EOFError()
        line = line.decode("utf-8")
        key, value = line.strip().split(",")

        return {key: value}

    def append(self, entry: str) -> int:
        line = entry + "\n"
        # Move to end of file
        self.file.seek(0, 2)
        # Get next free offset (where the entry will be written)
        offset = self.file.tell()
        # Write to offset
        self.file.write(line.encode("utf-8"))

        return offset
