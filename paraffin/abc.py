import typing as t

from dvc.stage import PipelineStage

HirachicalStages = t.Dict[int, t.List[PipelineStage]]
