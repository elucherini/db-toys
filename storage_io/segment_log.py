from pathlib import Path
from typing import Dict, List, Any

from storage_io.random_access_log import RandomAccessLogManager


class LogSegment(RandomAccessLogManager):
    """
    Class that handles log segments. Its functionality is equivalent to RandomAccessLogManager, with additional utils.
    """
    def tell(self):
        return self.file.tell()


