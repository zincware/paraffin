from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from paraffin.db import db_to_graph, list_experiments
from paraffin.utils import build_elk_hierarchy
import git

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
def read_experiments(commit: str|None = None):
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
