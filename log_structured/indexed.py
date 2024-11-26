from pathlib import Path
from typing import Dict

from storage_io.random_access_io import RandomAccessIO
from base.storage_engine import BaseStorageEngine


PATH = "log.txt"


class IndexedLogStructuredStorageEngine(BaseStorageEngine, RandomAccessIO):
    """
    Storage engine with in-memory index of key offset in file.
    """
    _data: Dict[str, int]

    def __init__(self):
        self._data = {}

    def get(self, key: str) -> str:
        if key not in self._data:
            raise ValueError(f"Key {key} not found in DB")
        offset = self._data[key]
        log = self.read_log(Path(PATH), offset)

        return log[key]

    def set(self, key: str, value: str) -> None:
        offset = self.append_to_log(Path(PATH), entry=f"{key},{value}")
        self._data[key] = offset

    @property
    def data(self):
        return self._data


def main():
    Path(PATH).unlink(missing_ok=True)

    storage = IndexedLogStructuredStorageEngine()
    storage.set("42", "{example example}")
    storage.set("10", "{another example}")
    assert storage.get("42") == "{example example}"
    assert storage.get("10") == "{another example}"
    storage.set("42", "{updated}")
    assert storage.get("42") == "{updated}"

    try:
        storage.get("nonexistent")
    except ValueError as e:
        print("Correctly got ValueError:", e)

    Path(PATH).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
