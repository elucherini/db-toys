from abc import abstractmethod
from pathlib import Path
from typing import Any, overload, List, Dict, Union


class BaseIO:
    """
    IO mixin
    """

    def read_log(self, path: Path, offset: Any) -> Any:
        pass

    def append_to_log(self, path: Path, entry: Any) -> None:
        pass
