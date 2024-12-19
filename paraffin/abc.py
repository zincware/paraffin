import dataclasses
import typing as t

from dvc.stage import PipelineStage


@dataclasses.dataclass(frozen=True)
class StageContainer:
    stage: PipelineStage
    branch: str
    origin: t.Optional[str]

    @property
    def name(self) -> str:
        return self.stage.name


HirachicalStages = t.Dict[int, t.List[StageContainer]]
