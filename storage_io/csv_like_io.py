from pathlib import Path
from typing import List, Dict

from base.base_io import BaseIO


class CSVLikeIO(BaseIO):
    """
    IO module that writes each entry on a separate line as "key,value" and
    """
    def read_log(self, path: Path, offset: None = None) -> List[Dict[str, str]]:
        with path.open(mode="r") as file:
            entries = file.readlines()

        log: List[Dict[str, str]] = []

        for entry in entries:
            clean_entry = entry.strip()
            if not clean_entry:
                continue
            # Split key-value by comma
            key, value = clean_entry.split(",")
            log.append({key: value})

        return log

    def append_to_log(self, path: Path, entry: str) -> None:
        with path.open(mode="a") as file:
            file.write(entry + "\n")
