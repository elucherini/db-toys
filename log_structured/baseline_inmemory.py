from typing import List, Dict

from base.base import BaseStorageEngine


class BaselineInMemoryLogStructuredStorageEngine(BaseStorageEngine):
    def __init__(self):
        self._data: List[Dict[str, str]] = []

    """
    Baseline log-structure in-memory storage engine. Any other purely in-memory storage engine must perform better.
    """
    def set(self, key: str, value: str) -> None:
        """
        Stores key-value pair in memory. O(1)
        """
        self.data.append({key: value})

    def get(self, key: str) -> str:
        """
        Retrieves value from given key. O(N).
        """
        for d in self.data:
            if key in d:
                return d[key]

    @property
    def data(self):
        return self._data


def main():
    storage = BaselineInMemoryLogStructuredStorageEngine()
    storage.set("42", "{example example}")
    storage.set("10", "{another example}")
    print(storage.get("42"))
    print(storage.get("10"))
    print(storage.data)


if __name__ == "__main__":
    main()
