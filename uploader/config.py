import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class UploadJob:
    local_dir: Path
    remote_dir: str

@dataclass(frozen=True)
class Parameter:
    LPI_SFTP_HOST: str
    LPI_SFTP_USER: str
    LPI_SFTP_PASSWORD: str

    HEALTH_SFTP_CONNECT: str
    HEALTH_SFTP_UPLOAD: str
    HEALTH_HASH: str

    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_DB: int

    UPLOAD_INTERVAL_SEC: int
    UPLOAD_JOBS: tuple[UploadJob, ...]

def _parse_jobs(raw: str) -> tuple[UploadJob, ...]:
    jobs: list[UploadJob] = []
    for item in [x.strip() for x in raw.split(",") if x.strip()]:
        local_s, remote_s = item.split(":", 1)
        jobs.append(UploadJob(local_dir=Path(local_s), remote_dir=remote_s))
    return tuple(jobs)

def get_parameter() -> Parameter:
    raw_jobs = os.getenv(
        "UPLOAD_JOBS",
        "/app/files/input1:/remote/input1,/app/files/input2:/remote/input2,/app/files/input3:/remote/input3",
    )
    return Parameter(
        LPI_SFTP_HOST=os.environ["LPI_SFTP_HOST"],
        LPI_SFTP_USER=os.environ["LPI_SFTP_USER"],
        LPI_SFTP_PASSWORD=os.environ["LPI_SFTP_PASSWORD"],

        HEALTH_SFTP_CONNECT=os.getenv("HEALTH_SFTP_CONNECT", "health:sftp_connect"),
        HEALTH_SFTP_UPLOAD=os.getenv("HEALTH_SFTP_UPLOAD", "health:sftp_upload"),
        HEALTH_HASH=os.getenv("HEALTH_HASH", "health:2501524_pipeline"),

        REDIS_HOST=os.getenv("REDIS_HOST", "redis"),
        REDIS_PORT=int(os.getenv("REDIS_PORT", "6379")),
        REDIS_DB=int(os.getenv("REDIS_DB", "0")),

        UPLOAD_INTERVAL_SEC=120,
        UPLOAD_JOBS=_parse_jobs(raw_jobs),
    )
