from abc import ABC, abstractmethod
from typing import Any, Union, Protocol, List, Dict, Optional


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...


class ProcessingPipeline(ABC):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id
        self.stages = List[ProcessingStage] = []
        self.processed: int = 0
        self.errors: int = 0
        self.next_pipeline: Optional["ProcessingPipeline"] = None

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    def set_next(self, pipeline: "ProcessingPipeline") -> None:
        self.next_pipeline = pipeline

    def execute_stages(self, data: Any) -> Any:
        try:
            for stage in self.stages:
                data = stage.process(data)
            self.processed += 1
            if self.next_pipeline is not None:
                return self.next_pipeline.process(data)
            return data
        except Exception as error:
            self.errors += 1
            print(f"Error in pipeline {self.pipeline_id}: {error}")
            return {"error": str(error)}

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        pass


class JSONAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        ...


class CSVAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        ...


class StreamAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Union[str, Any]:
        ...


class InputStage():
    def process(self, data: Any) -> Dict:
        processed = {}
        if isinstance(data, dict):
            for key, value in data.items():
                if key in ("sensor", "value", "unit"):
                    processed.update({key: value})

        elif isinstance(data, str):
            data_split = data.split(",")
            if len(data_split) % 3 == 0:
                tot_act = 0
                for item in data_split:
                    if item == "action":
                        tot_act += 1
            processed.update({"actions": tot_act})
            return processed

        elif isinstance(data, list):
            if len(data) <= 0:
                return {}
            processed.update({"len": len(data)})
            processed.update({"sum": sum(data)})
        return processed


class TransformStage():
    def process(self, data: Any) -> Dict:
        ...


class OutputStage():
    def process(self, data: Any) -> Any:
        ...


class NexusManager():
    ...


if __name__ == "__main__":
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")

    print("\nInitializing Nexus Manager...")
