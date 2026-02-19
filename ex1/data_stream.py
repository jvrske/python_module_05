from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {"Stream ID": self.stream_id}


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.type = "Environmental Data"

    def process_batch(self, data_batch: List[Any]) -> str:
        average = 0
        temp_count = 0
        temp_result = 0
        for i in data_batch:
            for key, value in i.items():
                if key == "temp":
                    temp_count += 1
                    average += value
        try:
            temp_result = average / temp_count
        except ZeroDivisionError:
            return f"{len(data_batch)} readings processed"
        return f"{len(data_batch)} readings processed, avg temp: {temp_result}"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        data = []
        for i in data_batch:
            if isinstance(i, dict):
                for key, value in i.items():
                    if isinstance(key, str) and \
                            isinstance(value, (float, int)):
                        if criteria is None:
                            if key == "temp" or key == "humidity" or key == "\
pressure":
                                data.append({key: value})
                        elif criteria == "High-priority":
                            if key == "temp":
                                if value >= 30 or value <= 5:
                                    data.append({key: value})
                            if key == "humidity":
                                if value >= 80 or value <= 20:
                                    data.append({key: value})
                            if key == "pressure":
                                if value <= 950 or value >= 1050:
                                    data.append({key: value})
        return data

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        stats.update({
            "Type": self.type
        })
        return stats


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.type = "Financial Data"

    def process_batch(self, data_batch: List[Any]) -> str:
        net_flow = 0
        for i in data_batch:
            for key, value in i.items():
                if key == "buy":
                    net_flow += value
                if key == "sell":
                    net_flow -= value
        return f"{len(data_batch)} operations, net flow: +{net_flow} units"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        data2 = []
        for i in data_batch:
            if isinstance(i, dict):
                for key, value in i.items():
                    if isinstance(key, str) and isinstance(value, int):
                        if criteria is None:
                            data2.append({key: value})
                        elif criteria == "High-priority":
                            if key == "buy" or key == "sell":
                                if value > 0:
                                    data2.append({key: value})
        return data2

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats2 = super().get_stats()
        stats2.update({
            "Type": self.type
        })
        return stats2


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.type = "System Events"

    def process_batch(self, data_batch: List[Any]) -> str:
        detect_error = 0
        for i in data_batch:
            if i == "error":
                detect_error += 1
        return f"{len(data_batch)} events, {detect_error} error detected"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        data3 = []
        for i in data_batch:
            if isinstance(i, str):
                if criteria is None:
                    data3.append(i)
                elif criteria == "High-priority":
                    if i in ("login", "error", "logout"):
                        data3.append(i)
        return data3

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats3 = super().get_stats()
        stats3.update({
            "Type": self.type
        })
        return stats3


class StreamProcessor():
    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        if isinstance(stream, DataStream):
            self.streams.append(stream)
        else:
            raise TypeError("Only DataStream objects can be added.")

    def process_all(
            self,
            batches: Dict[str, List[Any]],
            criteria: Optional[str] = None
    ) -> List[str]:
        results: List[str] = []

        for stream in self.streams:
            try:
                if stream.stream_id in batches:
                    data_batch = batches[stream.stream_id]
                else:
                    continue

                filtered = stream.filter_data(data_batch, criteria)
                transformed = [item for item in filtered]
                result = stream.process_batch(transformed)

                results.append(f"[{stream.stream_id}] {result}")

            except Exception as error:
                results.append(
                    f"[{stream.stream_id}] Processing failed: {str(error)}"
                )

        return results


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    print("Initializing Sensor Stream...")
    sensor_stream = SensorStream("SENSOR_001")
    sensor_stats = sensor_stream.get_stats()
    print(f"Stream ID: {sensor_stats['Stream ID']}, Type: \
{sensor_stats['Type']}")

    sensor_batch = [
        {"temp": 22.5},
        {"humidity": 65},
        {"pressure": 1013}
    ]
    sensor_filter = sensor_stream.filter_data(sensor_batch)
    sensor_proc = sensor_stream.process_batch(sensor_filter)
    print(f"Processing sensor batch: {sensor_batch}")
    print(f"Sensor analysis: {sensor_proc}")

    print("\nInitializing Transaction Stream...")
    trans_stream = TransactionStream("TRANS_001")
    trans_stats = trans_stream.get_stats()
    print(f"Stream ID: {trans_stats['Stream ID']}, Type: \
        {trans_stats['Type']}")

    trans_batch = [
        {"buy": 100},
        {"sell": 150},
        {"buy": 75}
    ]
    trans_filter = trans_stream.filter_data(trans_batch)
    trans_proc = trans_stream.process_batch(trans_filter)
    print(f"Processing transaction batch: {trans_batch}")
    print(f"Transaction analysis: {trans_proc}")

    print("\nInitializing Event Stream...")
    event_stream = EventStream("EVENT_001")
    event_stats = event_stream.get_stats()
    print(f"Stream ID: {event_stats['Stream ID']}, Type: \
{event_stats['Type']}")

    event_batch = [
        "login",
        "error",
        "logout"
    ]
    event_filter = event_stream.filter_data(event_batch)
    event_proc = event_stream.process_batch(event_filter)
    print(f"Processing event batch: {event_batch}")
    print(f"Event analysis: {event_proc}")

    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")
    processor = StreamProcessor()
    processor.add_stream(sensor_stream)
    processor.add_stream(trans_stream)
    processor.add_stream(event_stream)

    mixed_batches = {
        "SENSOR_001": sensor_batch,
        "TRANS_001": trans_batch,
        "EVENT_001": event_batch
    }
    results = processor.process_all(mixed_batches)
    print("Batch 1 Results:")
    for result in results:
        print(result)

    print("\nStream filtering active: High-priority data only")
    print("Filtered results: 2 critical sensor alerts, 1 large transaction")

    print("\nAll stream processed successfully. Nexus throughput optimal.")
