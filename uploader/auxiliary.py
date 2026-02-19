import time
from pathlib import Path
from typing import Optional

import paramiko

def newest_file(dirpath: Path) -> Optional[Path]:
    """
    Find the newst file my mtime in a folder.
    """
    files = [p for p in dirpath.iterdir() if p.is_file()]
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)
    
def is_file_stable(filepath: Path, settle_sec: float = 240.0) -> bool:
    """
    Avoid uploading a file that is still being written.
    """
    try:
        s1 = filepath.stat().st_size
        time.sleep(settle_sec)
        s2 = filepath.stat().st_size
        return s1 == s2
    except FileNotFoundError:
        return False
    
def remote_file_size(sftp: paramiko.SFTPClient, remote_path: str) -> Optional[int]:
    try:
        return sftp.stat(remote_path).st_size
    except FileNotFoundError:
        return None
    