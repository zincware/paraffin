"""Container for a DVC stage."""

import dataclasses
import json

from dvc.stage import PipelineStage
from dvc.stage.cache import _get_cache_hash



@dataclasses.dataclass(frozen=True, eq=True)
class PipelineStageDC:
    """Container for a DVC stage."""

    stage: PipelineStage
    status: str
    lock: str

    @property
    def changed(self) -> bool:
        """Check if the stage has changed."""
        return json.loads(self.status) != []

    @property
    def name(self) -> str:
        """Return the name of the stage."""
        return self.stage.name

    @property
    def cmd(self) -> str:
        """Return the command of the stage."""
        return self.stage.cmd
    
    @property
    def deps_lock(self) -> dict:
        """Return the hash of the dependencies."""
        lock = json.loads(self.lock)
        return {k: v for k, v in lock.items() if k in ["cmd", "params", "deps"]}
    
    @property
    def deps_hash(self) -> str:
        """Return the hash of the dependencies."""
        return _get_cache_hash(self.deps_lock, key=True)
