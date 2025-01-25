from pathlib import Path

import git
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from paraffin.db import (
    db_to_graph,
    get_job_dump,
    list_experiments,
    list_workers,
    update_job_status,
)
from paraffin.utils import build_elk_hierarchy

FILE = Path(__file__)

app = FastAPI()

app.mount("/ui", StaticFiles(directory=FILE.parent.parent / "static"), name="ui")
app.mount(
    "/assets",
    StaticFiles(directory=FILE.parent.parent / "static" / "assets"),
    name="assets",
)


@app.get("/")
def read_root():
    return RedirectResponse(url="/ui/index.html")


@app.get("/api/v1/experiments")
def read_experiments(commit: str | None = None):
    if commit is None:
        repo = git.Repo(search_parent_directories=True)
        commit = repo.head.commit.hexsha

    return list_experiments(commit=commit)


@app.get("/api/v1/graph")
def read_graph(experiment: str):
    g = db_to_graph(experiment_id=int(experiment))
    return build_elk_hierarchy(g)


@app.get("/api/v1/spawn")
def spawn():
    pass  # run paraffin worker


@app.get("/api/v1/job")
def read_job(name: str, experiment: int):
    return get_job_dump(job_name=name, experiment_id=int(experiment))


@app.get("/api/v1/workers")
def read_workers():
    return list_workers()


# update_job_status
@app.get("/api/v1/job/update")
def update_job(name: str, experiment: int, status: str = "pending"):
    return update_job_status(
        job_name=name, experiment_id=int(experiment), status=status
    )
