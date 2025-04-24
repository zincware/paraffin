import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from paraffin.db.app import get_stage_by_id, list_experiments, list_stages

# from paraffin.db import (
#     db_to_graph,
#     get_job_dump,
#     get_jobs,
#     list_experiments,
#     list_workers,
#     update_job_status,
# )
# from paraffin.utils import build_elk_hierarchy

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
    return list_experiments(db_url=db_url)


@app.get("/api/v1/stages")
def read_stages(experiment: str):
    db_url = os.environ["PARAFFIN_DB"]
    return list_stages(experiment_id=int(experiment), db_url=db_url)


@app.get("/api/v1/stage")
def read_stage(id: int):
    db_url = os.environ["PARAFFIN_DB"]
    return get_stage_by_id(stage_id=id, db_url=db_url)


# @app.get("/api/v1/graph")
# def read_graph(experiment: str):
#     db_url = os.environ["PARAFFIN_DB"]

#     g = db_to_graph(experiment_id=int(experiment), db_url=db_url)
#     return build_elk_hierarchy(g)


# @app.get("/api/v1/spawn")
# def spawn(
#     name: str | None = None, experiment: int | None = None, stage: str | None = None
# ):
#     # Build the command
#     cmd = ["paraffin", "worker"]
#     if name is not None:
#         cmd += ["--name", name]
#     if experiment is not None:
#         cmd += ["--experiment", str(experiment)]
#     if stage is not None:
#         cmd += ["--stage", stage]

#     # open subprocess and forget about it
#     subprocess.Popen(cmd)
#     return 0

#     # works but is slow
#     # print(f"Running command: {' '.join(cmd)}")

#     # # Start the subprocess and capture stdout and stderr
#     # process = subprocess.Popen(
#     #     cmd,
#     #     stdout=subprocess.PIPE,
#     #     stderr=subprocess.PIPE,
#     #     text=True,  # Ensure output is decoded as text
#     #     bufsize=1,  # Line-buffered output
#     #     universal_newlines=True,
#     # )

#     # # Define a generator to stream the output
#     # def generate():
#     #     while True:
#     #         # Read stdout line by line
#     #         output = process.stdout.readline()
#     #         if output:
#     #             yield f"stdout: {output}"
#     #         else:
#     #             break

#     #         # Read stderr line by line
#     #         error = process.stderr.readline()
#     #         if error:
#     #             yield f"stderr: {error}"
#     #         else:
#     #             break

#     #     # Wait for the process to complete
#     #     process.wait()
#     #     yield f"Process completed with return code: {process.returncode}"

#     # # Return a StreamingResponse to stream the output
#     # return StreamingResponse(generate(), media_type="text/plain")


# @app.get("/api/v1/job")
# def read_job(name: str, experiment: int):
#     db_url = os.environ["PARAFFIN_DB"]
#     return get_job_dump(job_name=name, experiment_id=int(experiment), db_url=db_url)


# # list finished, running, and pending and failed jobs
# @app.get("/api/v1/jobs")
# def read_jobs(experiment: int):
#     db_url = os.environ["PARAFFIN_DB"]
#     return get_jobs(experiment_id=int(experiment), db_url=db_url)


# @app.get("/api/v1/workers")
# def read_workers(id: int | None = None):
#     db_url = os.environ["PARAFFIN_DB"]
#     return list_workers(db_url=db_url, id=id)


# # update_job_status
# @app.get("/api/v1/job/update")
# def update_job(name: str, experiment: int, status: str, force: bool = False):
#     db_url = os.environ["PARAFFIN_DB"]
#     print(
#         f"Updating job {name} to {status} in experiment {experiment} with force={force}"
#     )
#     return update_job_status(
#         job_name=name,
#         experiment_id=int(experiment),
#         status=status,
#         db_url=db_url,
#         force=force,
#     )
