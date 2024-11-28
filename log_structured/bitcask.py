from pathlib import Path
from typing import Dict, List, Optional
import threading
from base.storage_engine import BaseStorageEngine
from storage_io.segment_log import LogSegment, LogSegmentManager


PATH = "."


class Bitcask(BaseStorageEngine):
    _data: List[Dict[str, int]]
    _log_manager: LogSegmentManager

    def __init__(self, path: str, max_segment_size: int = 1024 * 1024):
        self._log_manager = LogSegmentManager(path, max_segment_size=max_segment_size)
        super().__init__(path)
        self._data = self._log_manager.rebuild_index()

    def get(self, key: str) -> str:
        for i in range(len(self.data) - 1, -1, -1):
            if key in self.data[i]:
                offset = self.data[i][key]
                entry = self._log_manager.get_from_segment(i, offset)
                return entry[key]

        raise ValueError(f"Key {key} not found")

    def _update_index(self, key: str, offset: int):
        # If offset is 0, then a new segment has been created. Create new index
        if offset == 0:
            self.data.append({})
        self.data[-1][key] = offset

    def set(self, key: str, value: str) -> None:
        offset = self._log_manager.set(key, value)
        self._update_index(key, offset)

    def compact(self):
        self._log_manager.compact()
        self._data = self._log_manager.rebuild_index()

def main():
    bitcask = Bitcask(PATH, 20)

    bitcask.set("10", "write1")
    bitcask.set("42", "write1")
    print(bitcask.get("10"))
    print(bitcask.get("42"))
    bitcask.set("10", "write2")
    print(bitcask.get("10"))
    bitcask.set("12", "write1")
    bitcask.set("12", "write2")
    bitcask.set("11", "write1")
    print(bitcask.data)
    bitcask.compact()
    print(bitcask.data)


if __name__ == "__main__":
    main()
