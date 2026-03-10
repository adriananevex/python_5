from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List


class dataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return "Output: " + result


class NumericProcessor8dataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        if not isinstance(data, list):
            return False

        i = 0
        while i < self._count_items(data):
            v = data[i]
            if not isinstance(v, (int, float)):
                return False
            i += 1
        return True

    
