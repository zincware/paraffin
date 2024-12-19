import dataclasses
import typing as t

from dvc.stage import PipelineStage


@dataclasses.dataclass(frozen=True)
class StageContainer:
    stage: PipelineStage
    branch: str
    origin: t.Optional[str]
    commit: bool

    @property
    def name(self) -> str:
        return self.stage.name

    def to_dict(self) -> dict[str, t.Any]:
        dct = dataclasses.asdict(self)
        dct.pop("stage")
        dct["name"] = self.name
        return dct


HirachicalStages = t.Dict[int, t.List[StageContainer]]
