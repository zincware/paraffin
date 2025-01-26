"""Container for a DVC stage."""

import dataclasses
import json
import logging
import subprocess
import time

import dvc.api
from dvc.lock import LockError
from dvc.stage import PipelineStage
from dvc.stage.cache import _get_cache_hash
from dvc.stage.serialize import to_single_stage_lockfile

log = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True, eq=True)
class PipelineStageDC:
    """Container for a DVC stage."""

    stage: PipelineStage
    status: str

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


def retry(times, exceptions, delay: float = 0):
    """
    Retry Decorator
    Retries the wrapped function/method `times` times if the exceptions listed
    in ``exceptions`` are thrown
    :param times: The number of times to repeat the wrapped function/method
    :type times: Int
    :param Exceptions: Lists of exceptions that trigger a retry attempt
    :type Exceptions: Tuple of Exceptions
    """

    def decorator(func):
        def newfn(*args, **kwargs):
            attempt = 0
            while attempt < times:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    log.warning(f"Caught exception {e} - retrying {attempt}/{times}")
                    time.sleep(delay)
            return func(*args, **kwargs)

        return newfn

    return decorator


@retry(3, (LockError,), delay=0.5)
def get_lock(name: str) -> tuple[dict, str]:
    fs = dvc.api.DVCFileSystem(url=None, rev=None)
    with fs.repo.lock:
        stage = fs.repo.stage.collect(name)[0]
        stage.save(allow_missing=True, run_cache=False)
        stage_lock = to_single_stage_lockfile(stage, with_files=True)
        reduced_lock = {
            k: v for k, v in stage_lock.items() if k in ["params", "deps", "cmd"]
        }
        deps_hash = _get_cache_hash(reduced_lock, key=True)

    return stage_lock, deps_hash


def run_command(command: list[str]) -> tuple[int, str, str]:
    """Run a subprocess command, capturing its stdout, stderr, and return code."""
    popen = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )

    stdout_lines = []
    stderr_lines = []

    # Read stdout
    for stdout_line in iter(popen.stdout.readline, ""):
        print(stdout_line, end="")
        stdout_lines.append(stdout_line)
    popen.stdout.close()

    # Read stderr
    for stderr_line in iter(popen.stderr.readline, ""):
        print(stderr_line, end="")
        stderr_lines.append(stderr_line)
    popen.stderr.close()

    return_code = popen.wait()
    return return_code, "".join(stdout_lines), "".join(stderr_lines)


@retry(3, (LockError,), delay=0.5)
def repro(name: str) -> tuple[int, str, str]:
    """Reproduce a DVC stage.

    Parameters
    ----------
        name : str
            The name of the stage to reproduce.

    Returns
    -------
        Tuple[int, str, str]
            The return code, stdout, and stderr of the process.
    """
    stdout_lines = []
    stderr_lines = []

    # Run the main repro command
    return_code, repro_stdout, repro_stderr = run_command(
        ["dvc", "repro", "--single-item", name]
    )
    stdout_lines.append(repro_stdout)
    stderr_lines.append(repro_stderr)

    # Handle lock errors in repro
    if "ERROR: Unable to acquire lock" in repro_stderr:
        raise LockError(f"Unable to acquire lock for {name}")
    if f"ERROR: failed to reproduce '{name}': Unable to acquire lock" in repro_stderr:
        for _ in range(5):
            try:
                print(f"Committing {name} again due to lock error")
                commit_code, commit_stdout, commit_stderr = run_command(
                    ["dvc", "commit", name, "--force"]
                )
                stdout_lines.append(commit_stdout)
                stderr_lines.append(commit_stderr)
                if commit_code == 0:
                    return_code = 0  # we were able to commit the lock
                    break
            except subprocess.CalledProcessError:
                time.sleep(0.5)
        else:
            raise LockError(f"Unable to commit lock for {name}")

    # Combine and return outputs
    return return_code, "".join(stdout_lines), "".join(stderr_lines)
