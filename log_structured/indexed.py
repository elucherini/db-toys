from pathlib import Path
from typing import Dict

from storage_io.random_access_log import RandomAccessLogManager
from base.storage_engine import BaseStorageEngineWithIO


PATH = "log.txt"


class IndexedLogStructuredStorageEngine(BaseStorageEngineWithIO):
    """
    Storage engine with in-memory index of key offset in file.
    """
    _data: Dict[str, int]

    def __init__(self, path: str):
        self._data = {}
        self.io_manager = RandomAccessLogManager(path)
        super().__init__(path)

    def get(self, key: str) -> str:
        if key not in self._data:
            raise ValueError(f"Key {key} not found in DB")
        offset = self._data[key]
        log = self.io_manager.read(offset)

        return log[key]

    def set(self, key: str, value: str) -> None:
        offset = self.io_manager.append(entry=f"{key},{value}")
        self._data[key] = offset

    @property
    def data(self):
        return self._data


def main():
    # Clean up
    Path(PATH).unlink(missing_ok=True)

    storage = IndexedLogStructuredStorageEngine(PATH)
    storage.set("42", "{example example}")
    storage.set("10", "{another example}")
    assert storage.get("42") == "{example example}"
    assert storage.get("10") == "{another example}"
    storage.set("42", "{updated}")
    assert storage.get("42") == "{updated}"

    try:
        storage.get("nonexistent")
        # should not execute this line
        assert False
    except ValueError:
        pass

    # Clean up
    Path(PATH).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
