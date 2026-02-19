import time
import logging
import redis


logger = logging.getLogger(__name__)

DUE_ZSET = "alarm:pulse_due"
TOKEN_PREFIX = "alarm:pulse_token:"  # alarm:pulse_token:<hash>|<field>

def alarm_pulse(redis_db: redis.Redis, redis_hash: str, redis_key: str, duration_sec: int = 22):
    """
    Set alarm to 1 and schedule a distributed reset to 0 after duration_sec.
    """
    now = time.time()
    due = now + duration_sec

    member = f"{redis_hash}|{redis_key}"
    token_key = f"{TOKEN_PREFIX}{redis_hash}|{redis_key}"

    pipe = redis_db.pipeline(transaction=True)
    pipe.hset(redis_hash, redis_key, 1)
    pipe.set(token_key, str(due))
    pipe.zadd(DUE_ZSET, {member: due})
    pipe.execute()

    logger.debug(f"[redis] Pulsed {redis_hash}:{redis_key}=1, due reset at {due:.3f}.")
