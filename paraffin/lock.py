import re


def detect_zntrack(lock: dict) -> bool:
    """Detect if the lock is a ZnTrack lock."""
    return "zntrack" in lock.get("cmd", "")


def clean_lock(raw: dict) -> dict:
    """Clean the lock file for hashing.

    This function removes the "node-name" from the lock file,
    by cleaning: cmd, deps, outs and params.
    """
    exp = raw.copy()  # Copy the raw dictionary to avoid modifying the original

    # Extract the node name from the `cmd`
    node_name_match = re.search(r"--name\s+([\w_]+)", raw["cmd"])
    node_name = node_name_match.group(1) if node_name_match else None

    # Generalize `cmd`: replace the extracted node name with `<node-name>`
    if node_name:
        exp["cmd"] = raw["cmd"].replace(f"--name {node_name}", "--name <node-name>")

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

    # Generalize `outs` by removing `path` keys
    if "outs" in exp:
        exp["outs"] = [
            {"hash": out["hash"], out["hash"]: out[out["hash"]]} for out in exp["outs"]
        ]

    return exp


def update_lock(lock: dict, node_name: str) -> dict:
    """Update an existing lock with the new node name.

    If a cleaned lock hash is found in the database,
    take the respective lock and replace node name
    and nwd with the new node name.
    """
    return lock
