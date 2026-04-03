from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        """Process an input payload and return a formatted status."""

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Validate if input payload matches processor expectations."""

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, list):
            return False
        return all(isinstance(value, (int, float)) for value in data)

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                return self.format_output(
                    "ERROR: Invalid numeric data "
                    "(expected list of numbers)"
                )

            values = [value for value in data]
            count = len(values)
            total = sum(values)
            avg = total / count if count else 0.0

            return self.format_output(
                f"Processed {count} numeric values, "
                f"sum={total}, avg={avg}"
            )
        except Exception:
            return self.format_output(
                "ERROR: Unexpected failure while processing numeric data"
            )


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        return isinstance(data, str)

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                return self.format_output(
                    "ERROR: Invalid text data (expected string)"
                )

            chars = len(data)
            words = len(data.split())
            return self.format_output(
                f"Processed text: {chars} characters, {words} words"
            )
        except Exception:
            return self.format_output(
                "ERROR: Unexpected failure while processing text data"
            )


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        return isinstance(data, str) and ":" in data

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                return self.format_output(
                    "ERROR: Invalid log entry "
                    "(expected 'LEVEL: message')"
                )

            level, message = self._split_level_message(data)
            if level == "ERROR":
                return self.format_output(
                    f"[ALERT] ERROR level detected: {message}"
                )
            if level == "WARNING":
                return self.format_output(
                    f"[WARNING] WARNING level detected: {message}"
                )
            return self.format_output(
                f"[INFO] {level} level detected: {message}"
            )
        except Exception:
            return self.format_output(
                "ERROR: Unexpected failure while processing log data"
            )

    def _split_level_message(self, raw_log: str) -> tuple[str, str]:
        level, message = raw_log.split(":", 1)
        return level.strip(), message.strip()


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    print()
    print("Initializing Numeric Processor...")
    numeric_processor = NumericProcessor()
    numeric_data = [1, 2, 3, 4, 5]
    print(f"Processing data: {numeric_data}")
    if numeric_processor.validate(numeric_data):
        print("Validation: Numeric data verified")
    else:
        print("Validation: Invalid numeric data")
    print(numeric_processor.process(numeric_data))
    print()
    print("Initializing Text Processor...")
    text_processor = TextProcessor()
    text_data = "Hello Nexus World"
    print(f'Processing data: "{text_data}"')
    if text_processor.validate(text_data):
        print("Validation: Text data verified")
    else:
        print("Validation: Invalid text data")
    print(text_processor.process(text_data))
    print()
    print("Initializing Log Processor...")
    log_processor = LogProcessor()
    log_data = "ERROR: Connection timeout"
    print(f'Processing data: "{log_data}"')
    if log_processor.validate(log_data):
        print("Validation: Log entry verified")
    else:
        print("Validation: Invalid log entry")
    print(log_processor.process(log_data))
    print()
    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")

    processors: list[DataProcessor] = [
        NumericProcessor(),
        TextProcessor(),
        LogProcessor(),
    ]
    inputs: list[Any] = [[1, 2, 3], "Hello Nexus!", "INFO: System ready"]

    index = 0
    while index < len(processors):
        result = processors[index].process(inputs[index])
        print(f"Result {index + 1}: {result.replace('Output: ', '')}")
        index += 1
    print()
    print("Foundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
