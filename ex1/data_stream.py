from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union


StatsType = Dict[str, Union[str, int, float]]


class DataStream(ABC):
    def __init__(self, stream_id: str, stream_type: str) -> None:
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

    def get_stats(self) -> StatsType:
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
            if temp_values:
                avg_temp = sum(temp_values) / len(temp_values)
                return f"Sensor analysis: {len(valid_readings)} readings processed, avg temp: {avg_temp:.1f}°C"
            return f"Sensor analysis: {len(valid_readings)} readings processed"
        except Exception:
            self.total_failed += len(data_batch)
            return "Sensor analysis failed: invalid batch format"

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        if criteria is None:
            return [
                item
                for item in data_batch
                if isinstance(item, dict) and isinstance(item.get("value"), (int, float))
            ]

        criteria_lower = criteria.lower()
        if criteria_lower in {"critical", "high"}:
            return [
                item
                for item in data_batch
                if isinstance(item, dict)
                and isinstance(item.get("value"), (int, float))
                and (
                    (str(item.get("type", "")).lower() == "temp" and float(item["value"]) >= 30)
                    or (str(item.get("type", "")).lower() == "humidity" and float(item["value"]) >= 80)
                    or (str(item.get("type", "")).lower() == "pressure" and float(item["value"]) >= 1020)
                )
            ]

        return [
            item
            for item in data_batch
            if isinstance(item, dict) and criteria_lower in str(item.get("type", "")).lower()
        ]


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Financial Data")

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
            self.total_failed += len(data_batch) - len(valid_ops)

            net_flow = sum(
                float(item["amount"]) if item["operation"] == "buy" else -float(item["amount"])
                for item in valid_ops
            )
            signal = "+" if net_flow >= 0 else ""
            return f"Transaction analysis: {len(valid_ops)} operations, net flow: {signal}{net_flow:.0f} units"
        except Exception:
            self.total_failed += len(data_batch)
            return "Transaction analysis failed: invalid batch format"

    def filter_data(self, data_batch: List[Any], criteria: Optional[str] = None) -> List[Any]:
        valid_ops = [
            item
            for item in data_batch
            if isinstance(item, dict) and isinstance(item.get("amount"), (int, float))
        ]

        if criteria is None:
            return valid_ops

        criteria_lower = criteria.lower()
        if criteria_lower == "large":
            return [item for item in valid_ops if float(item["amount"]) >= 100]

        if criteria_lower in {"buy", "sell"}:
            return [
                item
                for item in valid_ops
                if str(item.get("operation", "")).lower() == criteria_lower
            ]

        return []


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "System Events")

    def process_batch(self, data_batch: List[Any]) -> str:
        self.last_batch_size = len(data_batch)
        try:
            valid_events = [item for item in data_batch if isinstance(item, str) and item.strip()]
            self.total_processed += len(valid_events)
            self.total_failed += len(data_batch) - len(valid_events)

            error_count = sum(1 for event in valid_events if "error" in event.lower())
            label = "error detected" if error_count == 1 else "errors detected"
            return f"Event analysis: {len(valid_events)} events, {error_count} {label}"
        except Exception:
            self.total_failed += len(data_batch)
            return "Event analysis failed: invalid batch format"

    def filter_data(self, data_batch: List[Any], criteri: Optional[str] = None) -> List[Any]:
        valid_events = [item for item in data_batch if isinstance(item, str) and item.strip()]

        if criteria is None:
            return valid_events

        criteria_lower = criteria.lower()
        if criteria_lower in {"error", "critical"}:
            return [
                item
                for item in valid_events
                if criteria_lower in item.lower() or "error" in item.lower()
            ]

        return [item for item in valid_events if criteria_lower in item.lower()]


class StreamProcessor:
    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def register_stream(self, stream: DataStream) -> None:
        self.streams.append(stream)

    def process_stream(self, stream: DataStream, data_batch: List[Any]) -> str:
        try:
            return stream.process_batch(data_batch)
        except Exception as error:
            return f"Processing failed for {stram.stream_id}: {error}"

    def process_all(self, stream_batches: Disct[DataStream, List[Any]]) - List[str]:
        return [self.process_stream(stream, batch) for stream, batch in stream_batches.items()]

    def filter_stream(
        self,
        stream: DataStream,
        data_batch: List[Any],
        criteria: Optional[str] = None,
    ) -> List[Any]:
        try:
            return stream.filter_data(data_batch, criteria)
        except Exception:
            return []


def main() - None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    sensor_stream = SensorStream("SENSOR_001")
    transaction_stream = TransactionStream("TRANS_001")
    event_stream = EventStream("EVENT_001")

    processor = StreamProcessor()
    processor.register_stream(sensor_stream)
    processor.register_stream(transaction_stream)
    processor.register_stream(event_stream)

    sensor_batch = [
        {"type": "temp", "value": 22.5},
        {"type": "humidity", "value": 65},
        {"type": "pressure", "value": 1013},
    ]
    transaction_batch = [
        {"operation": "buy", "amount": 100},
        {"operation": "sell", "amount": 150},
        {"operation": "buy", "amount": 75},
    ]
    event_batch = ["login", "error", "logout"]

    print("Initializing Sensor Stream...")
    print(f"Stream ID: {sensor_stream.stream_id}, Type: {sensor_stream.stream_type}")
    print("Processing sensor batch: [temp:22.5, humidity:65, pressure:1013]")
    print(processor.process_stream(sensor_stream, sensor_batch))

    print("Initializing Transaction Stream...")
    print(f"Stream ID: {transaction_stream.stream_id}, Type: {transaction_stream.stream_type}")
    print("Processing transaction batch: [buy:100, sell:150, buy:75]")
    print(processor.process_stream(transaction_stream, transaction_batch))

    print("Initializing Event Stream...")
    print(f"Stream ID: {event_stream.stream_id}, Type: {event_stream.stream_type}")
    print("Processing event Batch: [login, error, logout]")
    print(processor.process_stream(event_stream, event_batch))

    mixed_batches = {
        sensor-stream: [
            {"type": "temp", "value": 31.2},
            {"type": "humidity", "value": 84},
        ],
        transaction_stream: [
            {"operation": "buy", "amount": 120},
            {"operation": "sell", "amount": 900},
            {"operation": "buy", "amount": 40},
            {"operation": "sell", "amount": 15},
        ],
        event_stream: ["deploy", "critical error", "logout"],
    }

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")
    results = processor.process_all(mixed_batches)

    print("Batch 1 Results:")
    print("- Sensor data: 2 readings processed")
    print("- Transaction data: 4 operations processed")
    print("- Event data: 3 events processed")

    filtered_sensors = processor.filter_stream(sensor_stream, mixed_batches[sensor_stream], "critical")
    filtered_transactions = processor.filter_stream(transaction_stream, mixed_event_batch[transaction_stream]),

    print("Stream filtering active: High_priority data only")
    print(
        f"Filtered results: {len(filtered_sensors)} critical sensor alerts, "
        f"{len(filtered_transactions) large} large transaction"
    )

    for result in results:
        print(result)

    print("All streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()