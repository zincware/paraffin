import re
from collections import OrderedDict
from pathlib import Path

from paraffin.utils import get_group, replace_node_working_dir


def _extract_node_name(cmd: str) -> str | None:
    """Extract the node name from the command string."""
    node_name_match = re.search(r"--name\s+([\w_]+)", cmd)
    return node_name_match.group(1) if node_name_match else None


def _process_params(params: dict, node_name: str | None) -> dict:
    """Generalize the `params` field by replacing the node name with `<node-name>`."""
    generalized_params = {}
    for file, file_params in params.items():
        generalized_file_params = {}
        for key, value in file_params.items():
            # Only replace keys that match the extracted node name
            if key == node_name:
                generalized_file_params["<node-name>"] = value
            else:
                generalized_file_params[key] = value
        generalized_params[file] = generalized_file_params
    return generalized_params


def _process_deps(deps: list) -> list:
    """Generalize the `deps` field by removing the `path` field."""
    new_deps = []
    for dep in deps:
        new_dep = {"hash": dep["hash"]}
        if "files" in dep:
            new_dep["files"] = dep["files"]  # Keep `files` structure unchanged
        if "md5" in dep:
            new_dep["md5"] = dep["md5"]
        if "hash" in dep:
            new_dep["hash"] = dep["hash"]
        new_deps.append(new_dep)
    return new_deps


def clean_lock(raw: dict) -> dict:
    """Clean the lock file for hashing.

    This function removes the "node-name" from the lock file,
    by cleaning: cmd, deps, outs, and params.
    """
    exp = {k: v for k, v in raw.items() if k in ["cmd", "params", "deps"]}

    # Extract the node name from the `cmd`
    cmd_str = " ".join(raw["cmd"]) if isinstance(raw["cmd"], list) else raw["cmd"]
    node_name = _extract_node_name(cmd_str)

    # Generalize `cmd`: replace the extracted node name with `<node-name>`
    #  This only applies to ZnTrack commands.
    if node_name:
        # TODO: what if the list has more than one element?!
        generalized_cmd = cmd_str.replace(f"--name {node_name}", "--name <node-name>")
        exp["cmd"] = (
            [generalized_cmd] if isinstance(raw["cmd"], list) else generalized_cmd
        )

    # Generalize `params` if present
    if "params" in exp:
        exp["params"] = _process_params(raw["params"], node_name)

    # Generalize `deps` by removing `path`, but keeping `files`
    if "deps" in exp:
        exp["deps"] = _process_deps(raw["deps"])

    return exp


def _ordered_dict_to_dict(od: OrderedDict | dict) -> dict:
    """Convert a nested OrderedDict to a nested dict."""
    return {
        k: _ordered_dict_to_dict(v) if isinstance(v, OrderedDict) else v
        for k, v in od.items()
    }


def transform_lock(inp: dict, ref: dict) -> dict:
    """
    Transform the input lock based on the reference lock to
    produce the correct output lock.

    Parameters
    ----------
    inp : dict
        The input lock containing `cmd`, `params`, and `deps`.
    ref : dict
        The reference lock containing information about executed stages
        and their outputs.

    Returns
    -------
    dict
        The transformed lock that merges information from the input and reference.
    """
    # Extract the node name from `cmd` in the input lock
    inp_node_name_match = re.search(r"--name\s+([\w_]+)", inp["cmd"])
    if inp_node_name_match:
        inp_node_name = inp_node_name_match.group(1)
    else:
        raise ValueError("Node name not found in the input lock.")
    # Extract the node name from `cmd` in the reference lock
    ref_node_name_match = re.search(r"--name\s+([\w_]+)", ref["cmd"])
    if ref_node_name_match:
        ref_node_name = ref_node_name_match.group(1)
    else:
        raise ValueError("Node name not found in the reference lock.")

    # Detect groups
    inp_groups, isolated_inp_node_name = get_group(inp_node_name)
    ref_groups, isolated_ref_node_name = get_group(ref_node_name)

    inp_nwd = Path(*inp_groups, isolated_inp_node_name)
    ref_nwd = Path(*ref_groups, isolated_ref_node_name)

    # Transform the `outs` field using the reference lock
    outs = []
    for ref_out in ref.get("outs", []):
        # update the ref_nwd with the input nwd
        ref_out_path = Path(ref_out["path"])
        updated_out_path = replace_node_working_dir(ref_out_path, ref_nwd, inp_nwd)
        ref_out["path"] = updated_out_path.as_posix()
        outs.append(ref_out)

    inp["outs"] = outs

    return _ordered_dict_to_dict(inp)
