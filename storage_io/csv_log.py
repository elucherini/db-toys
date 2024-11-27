from pathlib import Path
from typing import List, Dict

from base.base_io import BaseIOManager


class CSVLogManager(BaseIOManager):
    """
    IO module that writes each entry on a separate line as "key,value" and
    """
    def __init__(self, path: str):
        super().__init__(path)

    def read(self, offset: None = None) -> List[Dict[str, str]]:
        self.file.seek(0)  # Reset file pointer to the beginning
        entries = self.file.readlines()

        log: List[Dict[str, str]] = []

        for entry in entries:
            key, value = entry.decode("utf-8").strip().split(",")
            log.append({key: value})

        return log

    def append(self, entry: str) -> None:
        line = entry + "\n"
        self.file.write(line.encode("utf-8"))
        self.file.flush()
