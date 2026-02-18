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
        str_len = len(data)
        words_list = len(data.split(" "))
        return f"Processed text: {str_len} characters, {words_list} words"

    def validate(self, data) -> bool:
        if isinstance(data, str):
            for i in data:
                if not isinstance(i, str):
                    return False
            return True
        return False

    def format_output(self, result):
        return super().format_output(result)


class LogProcessor(DataProcessor):
    def process(self, data) -> str:
        if "ERROR" in data:
            return "[ALERT] ERROR level detected: Connection timeout"
        if "INFO" in data:
            return "[INFO] INFO level detected: System ready"
        return None

    def validate(self, data):
        if isinstance(data, str):
            for i in data:
                if not isinstance(i, str):
                    return False
            return True
        return False

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
    else:
        print("Invalid input")

    print("Initializing Text Processor...")
    data2 = "Hello Nexus World"
    data2_proc = TextProcessor()

    print(f"Processing data: '{data2}'")
    if data2_proc.validate(data2):
        print("Validation: Text data verified")
        data2_final = data2_proc.process(data2)
        print(data2_proc.format_output(data2_final))
    else:
        print("Invalid input")

    print("\nInitializing Log Processor...")
    data3 = "ERROR: Connection timeout"
    print(f"Processing data: '{data3}'")

    data3_proc = LogProcessor()

    if data3_proc.validate(data3):
        print("Validation: Log entry verified")
        data3_final = data3_proc.process(data3)
        print(data3_proc.format_output(data3_final))

    print("\n=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")
    procs = [
        data1_proc,
        data2_proc,
        data3_proc
    ]
    datas = [
        [1, 2, 3],
        "Hello World!",
        "INFO level detected: System ready"
    ]

    result_count = 1
    for i in range(len(procs)):
        proc = procs[i]
        data = datas[i]

        if proc.validate(data):
            result = proc.process(data)
            print(f"Result {result_count}: {result.strip()}")
        else:
            print(f"Result {result_count}: Invalid input")

        result_count += 1

    print("\nFoundation systems online. Nexus ready for advanced streams.")
