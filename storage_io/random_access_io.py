from pathlib import Path
from typing import Dict

from base.base_io import BaseIO


class RandomAccessIO(BaseIO):
    """
    Random Access-like IO that reads a line at a given offset and appends to the file.
    """
    def read_log(self, path: Path, offset: int) -> Dict[str, str]:
        with path.open(mode="r") as file:
            file.seek(offset)
            entry = file.readline().rstrip("\n")

        key, value = entry.split(",")

        return {key: value}

    def append_to_log(self, path: Path, entry: str) -> int:
        with path.open(mode="a") as file:
            # Move to end of file
            file.seek(0, 2)
            # Get next free offset (where the entry will be written)
            offset = file.tell()
            # Write to offset
            file.write(entry + "\n")

        return offset
