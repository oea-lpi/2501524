import time
from pathlib import Path
from typing import Optional, Tuple


def next_stable_file_after_cursor(
    dirpath: Path,
    cursor_mtime: float,
    cursor_name: str,
    settle_sec: float = 600.0,
) -> Optional[Path]:
    """
    Return the stable file with the smallest (mtime, name) that is > (cursor_mtime, cursor_name).

    - Stable = "has not been modified for settle_sec seconds" (mtime age).
    - Cursor = last successfully uploaded file (mtime + name tie-breaker).
    Note: This scans the directory. For very large dirs, increase scan interval (UPLOAD_INTERVAL_SEC).
    """
    cursor_key: Tuple[float, str] = (cursor_mtime, cursor_name)
    now = time.time()

    best: Optional[Path] = None
    best_key: Optional[Tuple[float, str]] = None

    try:
        it = dirpath.iterdir()
    except FileNotFoundError:
        return None

    for p in it:
        if not p.is_file():
            continue

        try:
            st = p.stat()
        except FileNotFoundError:
            continue

        k: Tuple[float, str] = (float(st.st_mtime), p.name)
        if k <= cursor_key:
            continue
        if (now - float(st.st_mtime)) < settle_sec:
            continue
        if best_key is None or k < best_key:
            best = p
            best_key = k

    return best


def remote_file_size(sftp, remote_path: str) -> Optional[int]:
    try:
        return sftp.stat(remote_path).st_size
    except FileNotFoundError:
        return None