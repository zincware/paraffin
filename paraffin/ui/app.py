import os
from pathlib import Path

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
def read_experiments():
    db_url = os.environ["PARAFFIN_DB"]
    commit = os.getenv("PARAFFIN_COMMIT")

    return list_experiments(commit=commit, db_url=db_url)


@app.get("/api/v1/graph")
def read_graph(experiment: str):
    db_url = os.environ["PARAFFIN_DB"]

    g = db_to_graph(experiment_id=int(experiment), db_url=db_url)
    return build_elk_hierarchy(g)


@app.get("/api/v1/spawn")
def spawn():
    pass  # run paraffin worker


@app.get("/api/v1/job")
def read_job(name: str, experiment: int):
    db_url = os.environ["PARAFFIN_DB"]
    return get_job_dump(job_name=name, experiment_id=int(experiment), db_url=db_url)


@app.get("/api/v1/workers")
def read_workers():
    db_url = os.environ["PARAFFIN_DB"]
    return list_workers(db_url=db_url)


# update_job_status
@app.get("/api/v1/job/update")
def update_job(name: str, experiment: int, status: str = "pending"):
    db_url = os.environ["PARAFFIN_DB"]
    return update_job_status(
        job_name=name, experiment_id=int(experiment), status=status, db_url=db_url
    )
