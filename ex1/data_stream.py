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

    def filter_data(self, data_batch: List[Any], criteria=None) -> List[Any]:
        data = []
        for i in data_batch:
            if isinstance(i, dict):
                for key, value in i.items():
                    if isinstance(key, str) and isinstance(value, (float, int)):
                        if criteria is None:
                            if key == "temp" or key == "humidity" or key == "pressure":
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
    def __init__(self, stream_id):
        super().__init__(stream_id)
        self.type = "Financial Data"

    def process_batch(self, data_batch: str) -> None:
        super().process_batch(data_batch)

    def filter_data(self, data_batch: List[Any], criteria=None) -> List[Any]:
        data2 = []
        for i in data_batch:
            if isinstance(i, dict):
                for key, value in i.items():
                    if isinstance(key, str) and isinstance(value, int):
                        if key == "buy":
                            if value > 0 and value < 100:
                                data2.append({})

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats2 = super().get_stats()
        stats2.update({
            "Type": self.type
        })
        return stats2


class EventStream(DataStream):
    def process_batch(self, data_batch):
        return super().process_batch(data_batch)

    def filter_data(self, data_batch, criteria=None):
        return super().filter_data(data_batch, criteria)

    def get_stats(self):
        stats = super().get_stats()
        stats.update({
            "Type": self.type
        })
        return stats


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")

    print("Initializing Sensor Stream...")
    sensor_stream = SensorStream("SENSOR_001")
    sensor_stats = sensor_stream.get_stats()
    print(f"Stream ID: {sensor_stats['Stream ID']}, Type: {sensor_stats['Type']}")

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
    print(f"Stream ID: {trans_stats['Stream ID']}, Type: {trans_stats['Type']}")
    
    trans_batch = [
        {"buy": 100},
        {"sell": 150},
        {"buy": 75}
    ]
    trans_filter = trans_stream.filter_data(trans_batch)
    print(f"Processing transaction batch: {trans_batch}")