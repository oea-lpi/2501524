import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Parameter:
    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_DB: int

    DUE_ZSET: str
    TOKEN_PREFIX: str

def get_parameter() -> Parameter:
    return Parameter(
        REDIS_HOST=os.getenv("REDIS_HOST", "redis"),
        REDIS_PORT=int(os.getenv("REDIS_PORT", "6379")),
        REDIS_DB=int(os.getenv("REDIS_DB", "0")),
        DUE_ZSET="alarm:pulse_due",
        TOKEN_PREFIX="alarm:pulse_token:",
    )
