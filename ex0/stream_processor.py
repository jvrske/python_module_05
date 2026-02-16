from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def process(self, data) -> str:
        if len(data) > 0:
            data_len = len(data)
            data_sum = sum(data)
            data_avg = data_sum / data_len
            return f"Processed {data_len} numeric values, \
sum={data_sum}, avg={data_avg}\n"
        else:
            return "Invalid input"

    def validate(self, data) -> bool:
        if isinstance(data, list):
            for i in data:
                if not isinstance(i, int):
                    return False
            return True
        return False

    def format_output(self, result):
        return super().format_output(result)


class TextProcessor(DataProcessor):
    def process(self, data):
        return super().process(data)

    def validate(self, data):
        return super().validate(data)

    def format_output(self, result):
        return super().format_output(result)


class LogProcessor(DataProcessor):
    def process(self, data):
        return super().process(data)

    def validate(self, data):
        return super().validate(data)

    def format_output(self, result):
        return super().format_output(result)


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    data1 = [
        1, 2, 3, 4, 5
    ]

    print("Initializing Numeric Processor...")
    print(f"Processing data: {data1}")
    data1_proc = NumericProcessor()

    if data1_proc.validate(data1):
        print("Validation: Numeric data verified")
    data1_final = data1_proc.process(data1)
    print(data1_proc.format_output(data1_final))
