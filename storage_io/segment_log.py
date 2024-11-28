import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

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

    def __init__(self, path: str, max_segment_size: int = 1024 * 1024):
        self.path = Path(path)
        # Max size of the segments in bytes
        self._max_segment_size = max_segment_size
        self._segments = []
        self.lock = threading.Lock()   # To synchronize segment updates
        # If there are previous segments in the path, recover them
        self.recover_segments()

    @property
    def max_segment_size(self):
        """ Enforce read-only """
        return self._max_segment_size

    @property
    def segments(self):
        return self._segments

    def get_from_segment(self, segment_id: int, offset: int) -> Dict[str, str]:
        segment = self.segments[segment_id]

        with self.lock:
            if segment.file is None:
                # Open, then close if segment was closed
                segment.open()
                entry = segment.read(offset)
                segment.close()
            else:
                # Keep open if already open
                entry = segment.read(offset)
        return entry

    def get_segment_name(self) -> Path:
        """
        To avoid
        """
        timestamp = time.time()
        return self.path / f"{timestamp}.log"

    def _rollover_segment(self, curr_segment):
        """
        Roll over to new segment.
        """
        with self.lock:
            # Close current segment if it exists
            if curr_segment and curr_segment.is_open():
                curr_segment.close()
            # Instantiate new segment -- ID can be the current length of the segment list
        curr_segment = LogSegment(str(self.get_segment_name()))
        with self.lock:
            curr_segment.open()
            self.segments.append(curr_segment)

    def set(self, key: str, value: str) -> int:
        curr_segment = self.segments[-1] if self.segments else None
        if curr_segment and not curr_segment.is_open():
            curr_segment.open()
        if not curr_segment or curr_segment.tell() >= self.max_segment_size:
            # Current segment is full, generate new one
            self._rollover_segment(curr_segment)
        curr_segment = self.segments[-1]

        with self.lock:
            if not curr_segment.is_open():
                curr_segment.open()
            offset = curr_segment.append(entry=f"{key},{value}")

        # Update last index (corresponding to most recent segment)
        return offset

    def rebuild_index(self) -> List[Dict[str, int]]:
        index: List[Dict[str, int]] = []

        for curr_segment in self.segments:
            curr_dict = {}
            offset = 0

            if not curr_segment.is_open():
                curr_segment.open()

            while True:
                try:
                    entry = curr_segment.read(offset)
                    for key in entry.keys():
                        curr_dict[key] = offset
                    offset = curr_segment.tell()
                except EOFError:
                    break
            curr_segment.close()
            index.append(curr_dict)

        return index

    def recover_segments(self):
        """
        Searching for segments would normally be handled with file metadata. To keep things simple here, we use the
        simple requirement that all segments (and only the segments) in the directory end with .log
        """
        log_files = list(self.path.glob("*.log"))

        for file in log_files:
            segment = LogSegment(str(file))
            self._segments.append(segment)

    def compact(self):
        # Compact everything except for the active segment (last segment)
        segments = self.segments[:-1]

        # Collect all unique entries starting from most recent segment
        latest_entries = {}
        for curr_segment in segments:
            if not curr_segment.is_open():
                curr_segment.open()

            offset = 0
            while True:
                try:
                    entry = curr_segment.read(offset)
                    for key, value in entry.items():
                        latest_entries[key] = value
                    offset = curr_segment.tell()
                except EOFError:
                    break
            curr_segment.close()

        def create_compacted_segment():
            segment = LogSegment(str(self.get_segment_name()))
            segment.open()
            return segment

        compacted_segments: List[LogSegment] = [create_compacted_segment()]

        curr_segment: LogSegment = compacted_segments[0]
        for key, value in latest_entries.items():
            if curr_segment.tell() >= self.max_segment_size:
                # Rollover to new segment
                curr_segment.close()
                curr_segment = create_compacted_segment()
                compacted_segments.append(curr_segment)
            curr_segment.append(f"{key},{value}")

        # Replace atomically with compacted segments
        with self.lock:
            old_segments = self.segments[:-1].copy()
            self._segments = compacted_segments + self.segments[-1:]

        # Delete old files
        for segment in old_segments:
            segment.path.unlink()
