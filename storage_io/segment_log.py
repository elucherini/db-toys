import threading
from pathlib import Path
from typing import Dict, List, Any, Optional

from storage_io.random_access_log import RandomAccessLogManager
from base.base_io import BaseIOManager


class LogSegment(RandomAccessLogManager):
    """
    Class that handles a log segment. Its functionality is equivalent to RandomAccessLogManager, with additional utils.
    """
    def __init__(self, path: str):
        """
        Override RandomAccessLogManager __init__ so it doesn't open file DIR/log.txt.
        :param path:
        """
        BaseIOManager.__init__(self, path)

    def tell(self):
        return self.file.tell()


class LogSegmentManager:
    """
    Class to manage multiple log segments, including compacting utilities.
    """
    _segments: List[LogSegment]
    _max_segment_size: int
    _current_segment: Optional[LogSegment]

    def __init__(self, path: str, max_segment_size: int = 1024 * 1024):
        self.path = Path(path)
        # Max size of the segments in bytes
        self._max_segment_size = max_segment_size
        # Current segment
        self._current_segment = None
        self._segments = []
        self.lock = threading.Lock()   # To synchronize segment updates

    @property
    def max_segment_size(self):
        """ Enforce read-only """
        return self._max_segment_size

    @property
    def segments(self):
        return self._segments

    def get_from_segment(self, segment_id: int, offset: int) -> Dict[str, str]:
        with self.lock:
            segment = self._segments[segment_id]
            if segment.file is None:
                # Open, then close if segment was closed
                segment.open()
                entry = segment.read(offset)
                segment.close()
            else:
                # Keep open if already open
                entry = segment.read(offset)
            return entry

    def get_segment_name(self, _id: int | str) -> Path:
        return self.path / f"log{_id}{self.path.suffix}"

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

    def set(self, key: str, value: str) -> int:
        # Current segment is full, generate new one
        if not self._current_segment or self._current_segment.tell() >= self.max_segment_size:
            self._rollover_segment()

        with self.lock:
            offset = self._current_segment.append(entry=f"{key},{value}")

        # Update last index (corresponding to most recent segment)
        return offset

    # def compact(self):
    #     # Compact everything except for the active segment (last segment)
    #     segments = self.segments[:-1]
    #     segment_maps: List[Dict[str, int]] = self.bitcask.data
    #     new_segment = self._create_new_segment(self.bitcask.get_segment_name("0_compacted"))
    #
    #     seen_keys = set()
    #     for i in range(len(segments) - 1, -1, -1):
    #         curr_segment, curr_map = segments[i], segment_maps[i]
    #         for key in curr_map.keys():
    #             if key in seen_keys:
    #                 continue
    #             if new_segment.tell() >= self.bitcask.max_segment_size:
    #                 self._rollover_segment()
    #             # Retrieve key-value from old segment
    #             entry = curr_segment.read(curr_map[key])
    #             # Write to new segment
    #             new_segment.append(f"{key},{entry[key]}")
