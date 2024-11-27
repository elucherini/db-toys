from abc import abstractmethod, ABC
from pathlib import Path
from typing import Any
from base.base_io import BaseIOManager


class BaseStorageEngine(ABC):
    _data: Any
    path: Path

    def __init__(self, path: str):
        self.path = Path(path)

    @abstractmethod
    def get(self, key: Any) -> Any:
        pass

    @abstractmethod
    def set(self, key: Any, value: Any) -> None:
        pass

    @property
    def data(self):
        return self._data


class BaseStorageEngineWithIO(BaseStorageEngine, ABC):
    io_manager: BaseIOManager

    def __init__(self, path: str):
        super().__init__(path)
        self.io_manager.open()

    def close(self):
        self.io_manager.close()
