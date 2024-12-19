import typing as t
import dataclasses

from dvc.stage import PipelineStage


@dataclasses.dataclass(frozen=True)
class StageContainer:
    stage: PipelineStage

    @property
    def name(self) -> str:
        return self.stage.name

HirachicalStages = t.Dict[int, t.List[StageContainer]]
