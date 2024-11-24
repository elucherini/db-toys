from pathlib import Path
from typing import List, Dict

from base.base import BaseStorageEngine, BaseIO

"""
Basic log-structured storage engine that appends entries in a CSV-like format ("key,value"), one entry per line.
It retrieves the most recent entry for the desired key by going through the whole log in reverse order.
"""

PATH = "log.txt"


class CSVLikeIO(BaseIO):
    def read_log(self, path: Path) -> List[Dict[str, str]]:
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
            file.write("\n" + entry)


class BaselineLogStructuredStorageEngine(BaseStorageEngine, CSVLikeIO):
    """
    Baseline log-structure storage engine. Any other storage engine must perform better.
    """
    def set(self, key: str, value: str) -> None:
        """
        Stores key-value pair in memory. O(1)
        """
        self.append_to_log(Path(PATH), f"{key},{value}")

    def get(self, key: str) -> str:
        """
        Retrieves value from given key. O(N).
        """
        log = self.read_log(Path(PATH))

        for i in range(len(log) - 1, -1, -1):
            if key in log[i]:
                return log[i][key]

    def data(self):
        return self.read_log(Path(PATH))


def main():
    storage = BaselineLogStructuredStorageEngine()
    storage.set("42", "{example example}")
    storage.set("10", "{another example}")
    print(storage.get("42"))
    print(storage.get("10"))
    storage.set("42", "{updated}")
    print(storage.get("42"))
    print(storage.data())


if __name__ == "__main__":
    main()
