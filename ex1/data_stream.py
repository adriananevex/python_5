from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union


StatsType = Dict[str, Union[str, int, float]]

class DataStream(ABC):
    def __init__(self, stream_id: str, stream_type: str) ->None:
        self.stream_id: str = stream_id
        self.stream_type: str = stream_type
        self.total_processed: int = 0
        self.total_failed: int = 0
        self.last_batch_size: int = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass
    
    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        if criteria is None:
            return [item for item in data_batch if item is not None]
        criteria_lower = criteria.lower()
        return [item for item in data_batch if criteria_lower in str(item).lower()]

    def get_stats(self) -> StatsDict:
        return {
            "stream_id": self.stream_id,
            "stream_type": self.stream_type,
            "total_processed": self.total_processed,
            "total_failed": self.total_failed,
            "last_batch_size": self.last_batch_size,
        }

    
class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Environmental Data")

    def process_batch(self, data_batch: List[Any]) -> str:
        self.last_batch_size = len(data_batch)
        try:
            valid_readings = [
                item
                for item in data_batch
                if isinstance(item, dict) and isinstance(item.get("value"), (int, float))
            ]
            self.total_processed += len(valid_readings)
            self.total_failed += len(data_batch) - len(valid_readings)

            temp_values = [
                float(item["value"])
                for item in valid_readings
                if str(item.get("type", "")).lower() == "temp"
            ]
            if temp_values = [
                float(item["value"])
                for item in valid_readings
                if str(item.get("type", "")).lower() == "temp"
            ]
            if temp_values:
                avg_temp = sum(temp_values) / len(temp_values)
                return f"Sensor analytics: {len(valid_readings)} readings processed, avg temp: {avg_temp:.1f}°C"
            return f"Sensor analytis: {len(valid_readings)} readings processed"
        except Exception:
            self.total_failed += len(data_batch)
            return "Sensor analysis failed: invalid batch format"

    def filter_data(self, data_batch: List[Any], criteria: Option[str] = None) -> List[Any]:
        if criteria is None:
            return [
                item
                for item in data_batch
                if isinstance(item, dict) and isinstance(item.get("value"), (int, float))
            ]

        criteria_lower = criteria.lower()
        if criteria_lower == "critical":
            return [
                item
                for item in data_batch
                if isinstance(item, dict)
                and isinstance(item.get("value"), (int, float))
                and (
                    (str(item.get("type", "")).lower() == "temp" and float(item["value"]) >= 30)
                    or (str(item.get("type", "")).lower() == "humidity" and float(item["value"]) >= 80)
                )
            ]

        return [
            item
            for item in data_batch
            if isinstance(item, dict) and criteria_lower in str(item.get("type", "")).lower()
        ]


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Financial data")

    def process_batch(self, data_batch: List[Any]) -> str:
        self.last_batch_size = len(data_batch)
        try:
            valid_ops = [
                item
                for item in data_batch
                if isinstance(item, dict)
                and item.get("operation") in {"buy", "sell"}
                and isinstance(item.get("amount"), (int, float))
            ]
            self.total_processed += len(valid_ops)
            self.total_failed +=len(data_batch) - len(valid_ops)

            net_flow = sum(
                float(item["amount"]) if item["operation"] == "buy" else -float(item["amount"])
                for item in valid_ops
            )
            signal = 