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
        return {
            "name": self.name,
            "branch": self.branch,
            "origin": self.origin,
            "commit": self.commit,
        }


HirachicalStages = t.Dict[int, t.List[StageContainer]]
