from pathlib import Path
from typing import Dict, List, Optional

from base.storage_engine import BaseStorageEngine
from storage_io.segment_log import LogSegment


PATH = "log.txt"


class Bitcask(BaseStorageEngine):
    _data: List[Dict[str, int]]
    _segments: List[LogSegment]
    _max_segment_size: int
    _current_segment: Optional[LogSegment]

    def __init__(self, path: str, max_segment_size: int = 1024 * 1024):
        # Storage engine stuff
        self._data = []
        super().__init__(path)
        # Max size of the segments in bytes
        self._max_segment_size = max_segment_size
        # Current segment
        self._current_segment = None
        self._segments = []

    @property
    def max_segment_size(self):
        """ Enforce read-only """
        return self._max_segment_size

    def get(self, key: str) -> str:
        for i in range(len(self._segments) - 1, -1, -1):
            if key in self._data[i]:
                offset = self._data[i][key]
                segment = self._segments[i]
                if segment.file is None:
                    # Open, then close if segment was closed
                    segment.open()
                    entry = self._segments[i].read(offset)
                    segment.close()
                else:
                    # Keep open if already open
                    entry = self._segments[i].read(offset)
                return entry[key]

        raise ValueError(f"Key {key} not found")

    def get_segment_name(self, index: int) -> Path:
        return self.path.with_name(f"{self.path.stem}{index}{self.path.suffix}")

    def _rollover_segment(self):
        """
        Roll over to new segment.
        :return:
        """
        # Close current segment if it exists
        if self._current_segment:
            self._current_segment.close()
        # Instantiate new segment -- ID can be the current length of the segment list
        self._current_segment = LogSegment(str(self.get_segment_name(len(self._segments))))
        self._segments.append(self._current_segment)
        self._current_segment.open()
        # Initialize index for new segment
        self._data.append({})

    def set(self, key: str, value: str) -> None:
        # Current segment is full, generate new one
        if not self._current_segment or self._current_segment.tell() >= self.max_segment_size:
            self._rollover_segment()

        offset = self._current_segment.append(entry=f"{key},{value}")

        # Update last index (corresponding to most recent segment)
        self._data[-1][key] = offset


def main():
    bitcask = Bitcask("log.txt", 1)

    bitcask.set("10", "hello")
    bitcask.set("42", "hello")
    print(bitcask.get("10"))
    print(bitcask.get("42"))
    bitcask.set("10", "hello_new")
    print(bitcask.get("10"))


if __name__ == "__main__":
    main()
