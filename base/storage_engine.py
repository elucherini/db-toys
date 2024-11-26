from abc import abstractmethod, ABC
from typing import Any


class BaseStorageEngine(ABC):
    data: Any

    @abstractmethod
    def get(self, key: Any) -> Any:
        pass

    @abstractmethod
    def set(self, key: Any, value: Any) -> None:
        pass
