import time
import threading
from pathlib import Path

import paramiko
import redis

from auxiliary import next_stable_file_after_cursor, remote_file_size
from config import get_parameter
from helper.redis_utility import alarm_pulse

import json
from dataclasses import dataclass


@dataclass
class Cursor:
    mtime: float
    name: str


def load_cursor(path: Path) -> Cursor:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return Cursor(mtime=float(data.get("mtime", 0.0)), name=str(data.get("name", "")))
    except Exception:
        return Cursor(mtime=0.0, name="")


def save_cursor(path: Path, cursor: Cursor) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"mtime": cursor.mtime, "name": cursor.name}), encoding="utf-8")


def sftp_upload(sftp: paramiko.SFTPClient, local_file: Path, remote_dir: str) -> bool:
    """
    Upload with temp name + atomic rename:
      local -> remote/<name>.part -> rename to remote/<name>

    If remote final exists with same size => skip.
    If remote final exists with different size => write duplicate name suffix.
    """
    remote_dir = remote_dir.rstrip("/")
    final_path = f"{remote_dir}/{local_file.name}"
    tmp_path = f"{final_path}.part"

    local_size = local_file.stat().st_size

    remote_size = remote_file_size(sftp, final_path)
    if remote_size is not None and remote_size == local_size:
        return False

    try:
        sftp.remove(tmp_path)
    except FileNotFoundError:
        pass

    sftp.put(str(local_file), tmp_path)

    tmp_size = remote_file_size(sftp, tmp_path)
    if tmp_size != local_size:
        raise RuntimeError(f"Remote tmp size mismatch: {tmp_size} != {local_size}")

    remote_size = remote_file_size(sftp, final_path)
    if remote_size is not None and remote_size != local_size:
        ts = int(local_file.stat().st_mtime)
        final_path = f"{final_path}.dup_{ts}"

    sftp.rename(tmp_path, final_path)
    return True


def upload_newest_file(
    host: str,
    user: str,
    password: str,
    local_dir: Path,
    remote_dir: str,
    upload_interval_sec: int,
    redis_db: redis.Redis,
    health_hash: str,
    health_sftp_connect_key: str,
    health_sftp_upload_key: str,
    job_name: str,
) -> None:
    """
    Persistent, scalable uploader:
      - Uses a per-job cursor (mtime+name) so it doesn't need to remember all uploaded files.
      - Selects the next stable file after the cursor, ordered by (mtime, name).
      - Uploads via .part then rename to avoid partial final files.
      - Reconnects on failures with backoff.
    """
    state_path = Path("/app/logs") / f"cursor_{job_name}.json"
    cursor = load_cursor(state_path)

    settle_sec = 60.0 
    backoff = 2.0
    backoff_max = 45.0

    while True:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            print(f"[{job_name}] connecting {host} as {user}", flush=True)
            ssh.connect(host, 22, user, password, timeout=10)
            backoff = 2.0 
        except Exception:
            print(f"[{job_name}] connect failed; retry in {backoff:.0f}s", flush=True)
            alarm_pulse(redis_db, health_hash, f"{health_sftp_connect_key}:{job_name}")
            time.sleep(backoff)
            backoff = min(backoff * 2.0, backoff_max)
            continue

        ssh.get_transport().set_keepalive(30)
        sftp = ssh.open_sftp()

        uploaded_any = False
        try:
            while True:
                candidate = next_stable_file_after_cursor(
                    local_dir,
                    cursor_mtime=cursor.mtime,
                    cursor_name=cursor.name,
                    settle_sec=settle_sec,
                )
                if candidate is None:
                    break 

                try:
                    uploaded = sftp_upload(sftp, candidate, remote_dir)

                    st = candidate.stat()
                    cursor = Cursor(mtime=float(st.st_mtime), name=candidate.name)
                    save_cursor(state_path, cursor)

                    uploaded_any = True
                    if uploaded:
                        print(f"[{job_name}] uploaded {candidate.name}", flush=True)
                    else:
                        print(f"[{job_name}] already present {candidate.name}", flush=True)

                except Exception as e:
                    print(f"[{job_name}] upload failed ({candidate.name}): {e}", flush=True)
                    alarm_pulse(redis_db, health_hash, f"{health_sftp_upload_key}:{job_name}")
                    raise

            time.sleep(upload_interval_sec if not uploaded_any else 5)

        finally:
            try:
                sftp.close()
            except Exception:
                pass
            try:
                ssh.close()
            except Exception:
                pass

        time.sleep(min(upload_interval_sec, 5))


def main():
    para = get_parameter()

    redis_db = redis.Redis(
        host=para.REDIS_HOST,
        port=para.REDIS_PORT,
        db=para.REDIS_DB,
        decode_responses=True,
    )

    threads = []
    for job in para.UPLOAD_JOBS:
        job_name = job.local_dir.name
        t = threading.Thread(
            target=upload_newest_file,
            kwargs=dict(
                host=para.LPI_SFTP_HOST,
                user=para.LPI_SFTP_USER,
                password=para.LPI_SFTP_PASSWORD,
                local_dir=job.local_dir,
                remote_dir=job.remote_dir,
                upload_interval_sec=para.UPLOAD_INTERVAL_SEC,
                redis_db=redis_db,
                health_hash=para.HEALTH_HASH,
                health_sftp_connect_key=para.HEALTH_SFTP_CONNECT,
                health_sftp_upload_key=para.HEALTH_SFTP_UPLOAD,
                job_name=job_name,
            ),
            daemon=True,
            name=f"upload:{job_name}",
        )
        t.start()
        threads.append(t)

    while True:
        for t in threads:
            if not t.is_alive():
                raise RuntimeError(f"Uploader thread died: {t.name}")
        time.sleep(2)


if __name__ == "__main__":
    main()