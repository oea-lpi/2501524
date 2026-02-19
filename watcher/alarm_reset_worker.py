import os
import time
import logging

import redis

from config import get_parameter
#from logger.setup_logging import setup_logging


#logger = logging.getLogger("alarm_reset_worker")

def reset_loop(redis_db: redis.Redis, due_zset: str, token_prefix: str, poll_interval: float = 0.2, batch: int = 200):
    while True:
        now = time.time()

        members = redis_db.zrangebyscore(due_zset, min="-inf", max=now, start=0, num=batch)
        if not members:
            time.sleep(poll_interval)
            continue

        for member in members:
            # member format: "<hash>|<field>"
            try:
                redis_hash, redis_key = member.split("|", 1)
            except ValueError:
                #logger.warning(f"Bad member format: {member!r}")
                redis_db.zrem(due_zset, member)
                continue

            token_key = f"{token_prefix}{redis_hash}|{redis_key}"

            score = redis_db.zscore(due_zset, member)
            if score is None:
                continue

            token = redis_db.get(token_key)
            if token is None:
                # No token => clear and cleanup
                pipe = redis_db.pipeline(transaction=True)
                pipe.hset(redis_hash, redis_key, 0)
                pipe.zrem(due_zset, member)
                pipe.execute()
                continue

            try:
                latest_due = float(token)
            except ValueError:
                latest_due = None

            if latest_due is not None and abs(latest_due - float(score)) < 0.001:
                pipe = redis_db.pipeline(transaction=True)
                pipe.hset(redis_hash, redis_key, 0)
                pipe.zrem(due_zset, member)
                pipe.delete(token_key)
                pipe.execute()
                #logger.debug(f"Cleared {redis_hash}:{redis_key} -> 0")
            else:
                redis_db.zrem(due_zset, member)

if __name__ == "__main__":
    #setup_logging(process_name="alarm-reset-worker")
    para = get_parameter()

    redis_db = redis.Redis(
        host=para.REDIS_HOST,
        port=para.REDIS_PORT,
        db=para.REDIS_DB,
        decode_responses=True
    )
    reset_loop(redis_db, para.DUE_ZSET, para.TOKEN_PREFIX)
