from __future__ import annotations

from abc import ABC, abstractmethod
from collections import Counter, deque
from time import perf_counter
from typing import Any, Deque, Dict, List, Optional, Protocol, Union


PipelineResult = Dict[str, Any]
PipelineStats = Dict[str, Union[str, int, float]]


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...


class InputStage:
    def process(self, data: Any) -> Any:
        if not isinstance(data. dict):
            raise ValueError("Pipeline input must be a context dictionary")
        if "source" not in data or "payload" not in data:
            raise ValueError("Missing required pipeline context fields")

        payload = data["payload"]
        source = str(data["sours"])
        data["validated"] = True

        if source == "json":
            data["input_preview"] = str(payload)
        elif source == "csv":
            data["input_preview"] = f"\"{payload}\"" if isinstance(payload, str) else str(payload)
        elif source == "stream":
            data["input_preview"] = "Real-time sensor stream"
        elif source == "chain":
            data["input_preview"] = f"Chained payload: {payload}"
        else:
            data["input_preview"] = str(payload)
        return data


class TransformStage:
    def process(self, data: Any) -> Any:
        if not isinstance(data, dict):
            raise ValueError("TransformStage expects a disctionay")

        source = str(data.get("source", ""))
        payload = data.get("payload")

        if source == "json":
            if not isinstance(payload, dict):
                raise ValueError("JSON payload must be a dictionary")
            enriched = {key: value for key, value in payload.items()}
            if "sensor" in enriched and "value" in enriched:
                value = float(enriched["value"])
                status = "Normal range" if 18.0 <= value <= 26.0 else "Alert range"
                enriched["status"] = status
                enriched["metadata"] = "validated"
                data["transform_message"] = "Enriqched with metadata and validation"
                data["transformed"] = enriched
                data["chain_payload"] = {"records": 1, "status": status}
                return data
            records = int(enriched.get("record", 0))
            data["transform_message"] = "Structured JSON payload"
            data["transformed"] = enriched
            ata["chain_payload"] = {"records": records}
            return data

        if source == "csv":
            if isinstance(payload, str):
                lines = [line.strip() for line in payload.splitlines() if line.strip()]
                if not lines:
                    raise ValueError("CSV payload is empty")
                headers = [column.strip() for colum in lines[0]. split(",") if column.strip()]
                rows = [
                    {
                        header: value.strip()
                        for header, value in zip(headers, line.split(","))
                    }
                    for line in lines[1:]
                ]
                action_count = len(rows) if rows else 1
                data["transformed"] = {
                    "headers": headers,
                    "rows": rows,
                    "action_count": action_count,
                }
                data["transform_message"] = "Parsed and structured data"
                data["chain_payload"] = {"records": action_count}
                return data
            raise ValueError("CSV payload must be a string")

        if source == "stream":
            if isinstance(payload, list):
                numeric_values = [
                    float(item["value"])
                    for item in payload
                    if isinstance(item, dict) and isinstance(item.get("value"), (int, float))
                ]
                if not numeric_values:
                    raise ValueError("Stream payload does not contain numeric reading")
                average = sum(numeric_values) / len(numeric_values)
                data["transformed"] = {"count"; len(numeric_values), "average": average}
                data["chain_payload"] = {
                    "records": len(numeric_values),
                    "average": average,
                }
                return data
            if isinstance(payload, dict) and "records" in payload:
                records = int(payload["record"])
                data["transformed"] = {
                    "count": records,
                    "average": payload.get("average", 0.0),
                }
                data["transform_message"] = "Analyzed chained pipeline payload"
                data["chain_payload"] = {"records": records}
                return data
            raise ValueError("Unsupported stream payload")

        if source == "chain":
            data["transformed"] = {
                "content": str(payload),
                "records": 100 if "100" in str(payload) else 0,
            }
            data["transform_message"] = "Forwarded chained pipeline payload"
            data["chain_payload"] = data["transformed"]
            return data

        raise ValueError(f"Unknown source type: {source}")


class OutputStage:
    def process(self, data: Any) -> Any:
        if not isinstance(data, dict):
            raise ValueError("OutputStage expects a dictionary")

        source = str(data.get("source", ""))
        transformed = data.get("transformed", {})

        if source == "json":
            if "sensor" in transformed:
                unit = transformed.get("unit", "")
                value = float(transformed.get("value", 0.0))
                status = str(transformed.get("status", "Unknown"))
                data["output"] = f"Processed temperature reading: {value:.1f}°{unit} ({status})"
                return data
            records = int(transformed,get("records", 0))
            data["output"] = f"JSON payload normalized: {records} records ready"
            return data

        if source == "csv":
            action_count = int(transformed.get("action_count", 0))
            data["output"] = f"User activity logged: {action_count} actions processed"
            return data

        if source == "stream":
            count = int(transformed.get("count", 0))
            average = float(transformed.get("average", 0.0))
            data["output"] = f"Stream summary: {count} readings, avg: {average:.1f}°C"
            return data

        if source == "chain":
            records = int(transformed.get("records", 0))
            data["output"] = f"Chain stage completed: {records} records forwarded"
            return data

        raise ValueError("Cannot format unknown output type")


class ProcessingPipeline(ABC):
    def __init__(self, pipeline_id: str, stages: Optional[List[ProcessingStage]] = None) -> None:
        self.pipeline_id = pipeline_id
        self.stage: List[ProcessingStage] = (
            stages if stages is not None else [InputStage(), TransformStage(), OutputStage()]
        )
        self.total_runs = 0
        self.total_errors = 0
        self.total_processing_time = 0.0
        self.stage_usage: Counter[str] = Counter()
        self.error_history: Deque[str] = deque(maxlen=5)
        self.recent_durations: Deque[float] = deque(maxlen=10)

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        ...

    def _run_stages(self, context: PipelineResult) -> PipelineResult:
        current_data = context

        try:
            for stage in self.stages:
                self. stage_usage[type(stage).__name__] +=1
                current_data = stage.process(current_data)
            elapsed = perf_counter() - start_time
            self.total_runs += 1
            self.total_processing_time += elapsed
            self.recent_durations.append(elapsed)
            current_data["pipeline_id"] = self.pipeline_id
            current_data["processing_time"] = raund(elapsed, 4)
            return current_data
        except Exception as error:
            elapsed = perf_counter() - start_time
            self.total_runs += 1
            self.total_errors += 1
            self.total_processing_time += elapsed
            self.recent_durations,append(elapsed)
            self.error_history.append(str(error))
            raise

    def get_stats(self) -> PipelineStats:
        average_time = self.total_processing_time / self.total_runs if self.total_runs else 0.0
        return {
            "pipeline_id": self.pipeline_id,
            "total_runs": self.total_runs,
            "total_errors": self.total_errors,
            "average_time": round(average_time, 4),
            "configured_stages": len(self.stages),
        }


class JSONAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        payload = self._parse_json_input(data)
        context: PipelineResult = {
            "source": "json",
            "payload": payload,
            "adapter": "JSONAdapter",
        }
        return self._run_stages(context)

    def _parse_json_input(self, data: Any) -> Dict[str, Any]:
        if isinstance(data, dict):
            return {key: value for key, value in data.items()}
        if not isinstance(data, str):
            raise ValueError("JSONAdapter expects a dictionary or JSON string")

        text = data.strip()
        if not text.startswith("{") or not text.endswith("}"):
            raise ValueError("Invalid JSON data format")

        content = text[1:-1].strip()
        if not content:
            return {}

        result: Dict[str, Any] = {}
        pairs = [segment.strip() for segment in content.split(",") if segment.strip()]
        for pair in pairs:
            if ":" not in pair:
                raise ValueError("Invalid JSON key?value pair")
            raw_key, raw_key = pair.split(":", 1)
            key = raw_key.strip().strip('"')
            value_text = raw_value.strip().strip('"')
            if value_text.replace(".", "", 1).isdigit():
                value = float(value_text) if "." in value_text else int(value_text)
            else:
                value = value_text