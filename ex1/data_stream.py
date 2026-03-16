from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):

    def __init__(self, stream_id: str) -> None:
        self.stream_id: str = stream_id
        self.processed_count: int = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:

        pass

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria is None:
            return data_batch

        return [d for d in data_batch if criteria in str(d)]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:

        return {
            "stream_id": self.stream_id,
            "processed": self.processed_count
        }


class SensorStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            temps: List[float] = [
                float(d.split(":")[1])
                for d in data_batch
                if isinstance(d, str) and d.startswith("temp:")
            ]

            self.processed_count += len(data_batch)

            avg_temp: float = sum(temps) / len(temps) if temps else 0

            return (
                f"Sensor data: {len(data_batch)} readings processed, "
                f"avg temp: {avg_temp}°C"
            )

        except Exception as e:
            return f"Sensor stream error: {e}"


class TransactionStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            flow: List[float] = []

            for d in data_batch:
                if not isinstance(d, str) or ":" not in d:
                    continue

                op, value = d.split(":")
                value = float(value)

                if op == "buy":
                    flow.append(-value)
                elif op == "sell":
                    flow.append(value)

            self.processed_count += len(data_batch)

            net_flow: float = sum(flow)

            return (
                f"Transaction data: {len(data_batch)} operations processed, "
                f"net flow: {net_flow:+} units"
            )

        except Exception as e:
            return f"Transaction stream error: {e}"


class EventStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            errors: List[str] = [
                e for e in data_batch
                if isinstance(e, str) and "error" in e.lower()
            ]

            self.processed_count += len(data_batch)

            return (
                f"Event data: {len(data_batch)} events processed, "
                f"{len(errors)} error detected"
            )

        except Exception as e:
            return f"Event stream error: {e}"


class StreamProcessor:

    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:

        self.streams.append(stream)

    def process_all(self, batches: List[List[Any]]) -> None:

        print("\n=== Polymorphic Stream Processing ===")
        print("Processing mixed stream types through unified interface...\n")

        for stream, batch in zip(self.streams, batches):
            result: str = stream.process_batch(batch)
            print(result)


def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    print("\nInitializing Sensor Stream...")
    sensor: SensorStream = SensorStream("SENSOR_001")

    print("Initializing Transaction Stream...")
    transaction: TransactionStream = TransactionStream("TRANS_001")

    print("Initializing Event Stream...")
    event: EventStream = EventStream("EVENT_001")

    processor: StreamProcessor = StreamProcessor()

    processor.add_stream(sensor)
    processor.add_stream(transaction)
    processor.add_stream(event)

    batches: List[List[Any]] = [
        ["temp:22.5", "humidity:65", "pressure:1013"],
        ["buy:100", "sell:150", "buy:75"],
        ["login", "error", "logout"]
    ]

    processor.process_all(batches)

    print("\nAll streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()
