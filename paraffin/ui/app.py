from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from paraffin.db import db_to_graph
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


@app.get("/api/v1/graph")
def read_graph():
    g = db_to_graph()
    return build_elk_hierarchy(g)


@app.get("/api/v1/spawn")
def spawn():
    pass  # run paraffin worker
