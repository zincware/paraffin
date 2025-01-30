import re
from collections import OrderedDict
from pathlib import Path

from paraffin.utils import get_group, replace_node_working_dir


def clean_lock(raw: dict) -> dict:
    """Clean the lock file for hashing.

    This function removes the "node-name" from the lock file,
    by cleaning: cmd, deps, outs, and params.
    """
    exp = {k: v for k, v in raw.items() if k in ["cmd", "params", "deps"]}

    # Extract the node name from the `cmd`
    cmd_str = " ".join(raw["cmd"]) if isinstance(raw["cmd"], list) else raw["cmd"]
    node_name_match = re.search(r"--name\s+([\w_]+)", cmd_str)
    node_name = node_name_match.group(1) if node_name_match else None

    # Generalize `cmd`: replace the extracted node name with `<node-name>`
    if node_name:
        generalized_cmd = cmd_str.replace(f"--name {node_name}", "--name <node-name>")
        exp["cmd"] = (
            [generalized_cmd] if isinstance(raw["cmd"], list) else generalized_cmd
        )

    # Generalize `params` if present
    if "params" in exp:
        generalized_params = {}
        for file, file_params in exp["params"].items():
            generalized_file_params = {}
            for key, value in file_params.items():
                # Only replace keys that match the extracted node name
                if key == node_name:
                    generalized_file_params["<node-name>"] = value
                else:
                    generalized_file_params[key] = value
            generalized_params[file] = generalized_file_params
        exp["params"] = generalized_params

    # Generalize `deps` by removing `path` keys
    if "deps" in exp:
        exp["deps"] = [
            {"hash": dep["hash"], dep["hash"]: dep[dep["hash"]]} for dep in exp["deps"]
        ]

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
