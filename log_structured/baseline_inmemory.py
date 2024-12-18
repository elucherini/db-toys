from typing import List, Dict
from base.base_io import BaseIOManager
from base.storage_engine import BaseStorageEngineWithIO


class DummyIOManager(BaseIOManager):
    """
    This storage engine is purely in-memory and doesn't need IO
    """
    def open(self):
        pass


class BaselineInMemoryLogStructuredStorageEngine(BaseStorageEngineWithIO):
    """
    Baseline log-structure in-memory storage engine. Any other purely in-memory storage engine must perform better.
    """

    def __init__(self, path: str = ""):
        self._data: List[Dict[str, str]] = []
        self.io_manager = DummyIOManager(path)
        super().__init__(path)

    def set(self, key: str, value: str) -> None:
        """
        Stores key-value pair in memory. O(1)
        """
        self.data.append({key: value})

    def get(self, key: str) -> str:
        """
        Retrieves value from given key. O(N).
        """
        for i in range(len(self.data) - 1, -1, -1):
            if key in self.data[i]:
                return self.data[i][key]

    @property
    def data(self):
        return self._data


def main():
    storage = BaselineInMemoryLogStructuredStorageEngine()
    storage.set("42", "{example example}")
    storage.set("10", "{another example}")
    print(storage.get("42"))
    print(storage.get("10"))
    storage.set("42", "{updated}")
    print(storage.get("42"))
    print(storage.data)


if __name__ == "__main__":
    main()
