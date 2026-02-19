import logging
import time
import threading
from pathlib import Path
from typing import Optional

import paramiko
import redis

from auxiliary import newest_file, is_file_stable, remote_file_size
from config import get_parameter
#from logger.setup_logging import setup_logging
from helper.redis_utility import alarm_pulse

#logger = logging.getLogger("uploader")


def sftp_upload(sftp: paramiko.SFTPClient, local_file: Path, remote_dir: str) -> bool:
    remote_final_path = f"{remote_dir.rstrip('/')}/{local_file.name}"

    remote_size = remote_file_size(sftp, remote_final_path)
    local_size = local_file.stat().st_size
    if remote_size is not None and remote_size == local_size:
        return False
    elif remote_size is not None and remote_size != local_size:
        ts = int(local_file.stat().st_mtime)
        remote_final_path = f"{remote_final_path}.dup_{ts}"

    sftp.put(str(local_file), remote_final_path)
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
    while True:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy()) 

        try:
            print(f"[{job_name}] {host}, {user}", flush=True)
            ssh.connect(host, 22, user, password, timeout=10)
        except Exception:
            #logger.exception(f"[{job_name}] Could not connect to remote SFTP-server.")
            print("Cant connect.", flush=True)
            alarm_pulse(redis_db, health_hash, f"{health_sftp_connect_key}:{job_name}")
            time.sleep(upload_interval_sec)
            continue

        print("Do I even get here?", flush=True)
        ssh.get_transport().set_keepalive(30)
        sftp = ssh.open_sftp()

        try:
            last_uploaded_name: Optional[str] = None
            while True:
                new_file = newest_file(local_dir)
                if new_file and is_file_stable(new_file):
                    try:
                        if new_file.name != last_uploaded_name:
                            if sftp_upload(sftp, new_file, remote_dir):
                                last_uploaded_name = new_file.name
                                #logger.debug(f"[{job_name}] Uploaded {new_file.name}.")
                    except Exception:
                        #logger.exception(f"[{job_name}] Upload failed for {new_file.name}, skip.")
                        alarm_pulse(redis_db, health_hash, f"{health_sftp_upload_key}:{job_name}")

                time.sleep(upload_interval_sec)

        finally:
            try:
                sftp.close()
            except Exception:
                pass
            ssh.close()

        time.sleep(upload_interval_sec)


def main():
    #setup_logging(process_name="uploader")
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
                remote_dir=job.remote_dir,  # keep as str
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
        #logger.info(f"Started uploader for {job.local_dir} -> {job.remote_dir}")

    while True:
        for t in threads:
            if not t.is_alive():
                raise RuntimeError(f"Uploader thread died: {t.name}")
        time.sleep(2)


if __name__ == "__main__":
    main()
