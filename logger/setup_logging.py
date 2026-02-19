import json
import logging.config
from pathlib import Path

def _rewrite_filename(orig: str, process_name: str, suffix: str) -> str:
    """
    Turn logs/app.log.jsonl into logs/app_{process}_{suffix}.log.jsonl
    Example: app + container + debug -> app_container_debug.log.jsonl
    """
    p = Path(orig)
    name = p.name

    if "." in name:
        root, rest = name.split(".", 1)
        rest = "." + rest 
    else:
        root, rest = name, ""

    new_name = f"{root}_{process_name}_{suffix}{rest}"
    return str(p.parent / new_name)

def setup_logging(process_name: str) -> None:
    """
    Set up Python logging using a JSON configuration file.

    Args:
        process_name (str): A name representing the current process.
    """
    with open("logger/logger_config.json") as f:
        config = json.load(f)

    # Rewrite info/debug file name
    info_key = "file_info"
    if info_key in config["handlers"]:
        fn = config["handlers"][info_key].get("filename", "logs/app_info.log.jsonl")
        config["handlers"][info_key]["filename"] = _rewrite_filename(fn, process_name, "debug")

    # Rewrite warning+ file name
    err_key = "file_error"
    if err_key in config["handlers"]:
        fn = config["handlers"][err_key].get("filename", "logs/app_error.log.jsonl")
        config["handlers"][err_key]["filename"] = _rewrite_filename(fn, process_name, "error")

    logging.config.dictConfig(config)