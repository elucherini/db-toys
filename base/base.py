from abc import abstractmethod, ABC
from pathlib import Path
from typing import Any


class BaseStorageEngine(ABC):
    @abstractmethod
    def get(self, key: Any) -> Any:
        pass

    @abstractmethod
    def set(self, key: Any, value: Any) -> None:
        pass


class BaseIO(ABC):
    """
    IO mixin
    """

    @abstractmethod
    def read_log(self, path: Path) -> Any:
        pass

    @abstractmethod
    def append_to_log(self, path: Path, entry: Any) -> None:
        pass
