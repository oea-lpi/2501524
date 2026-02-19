from collections import deque
import json
import logging
import os
from pathlib import Path
import tempfile
import time
import threading

import docker
import modbus_server
import redis

from logger.setup_logging import setup_logging


logger = logging.getLogger("modbus")

MODBUS_HOST = os.getenv("MODBUS_HOST", "0.0.0.0")
MODBUS_PORT = int(os.getenv("MODBUS_PORT", 502))
PERSIST_PATH = Path("/app/files/last_registers.csv")

docker_client = docker.DockerClient(
    base_url='unix:///var/run/docker.sock',
    timeout=3
)

def load_persisted() -> dict[int, float]:
    """
    Load last-written Modbus register values from a CSV file. Reads 'path/last_registers.csv' where each line is 'register,value'.
    Malformed lines are ignored. If the file does not exist, an empty mapping is returned. This data is used at startup to prefill holding registers so
    they retain their last values across restarts.

    Returns:
        dict[int, float]: A mapping of Modbus register addresses to their last persisted float values. Empty if the persistence file does not exist.
    """
    data = {}
    if not PERSIST_PATH.exists():
        return data
    try:
        with PERSIST_PATH.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                try:
                    reg_str, val_str = line.split(",", 1)
                    reg = int(reg_str.strip())
                    val = float(val_str.strip())
                    data[reg] = val
                except Exception:
                    # ignore malformed lines
                    continue
    except Exception:
        logger.exception("Failed to read persisted registers file.")
    return data

def save_persisted(data: dict[int, float]) -> None:
    """
    Persist last-written Modbus register values to disk atomically.
    Writes the provided 'registe' to 'value' mapping to 'PERSIST_PATH/last_registers.csv' using a temporary file and atomic rename.

    Args:
        data: Mapping of Modbus register addresses to the float values that should be persisted.
    """
    try:
        PERSIST_PATH.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", delete=False, dir=str(PERSIST_PATH.parent), encoding="utf-8") as tmp:
            for reg in sorted(data.keys()):
                tmp.write(f"{reg},{data[reg]}\n")
            tmp_path = Path(tmp.name)
        tmp_path.replace(PERSIST_PATH)
    except Exception:
        logger.exception("Failed to write persisted registers file.")

class RegisterWriter:
    """
    Wrapper around the Modbus server that caches and persists writes.
    - cache: In-memory mapping of register -> float value
    """
    def __init__(self, server, persist_interval: float = 10.0):
        self.server = server
        self.cache: dict[int, float] = load_persisted()
        self._lock = threading.Lock()
        self._persist_interval = persist_interval

        t = threading.Thread(target=self._persist_loop, daemon=True, name="persist_registers")
        t.start()

    def _persist_loop(self) -> None:
        """
        Writes current cache every persistent_interval to disk.
        """
        while True:
            time.sleep(self._persist_interval)
            try:
                with self._lock:
                    if self.cache:
                        save_persisted(self.cache)
            except Exception:
                logger.exception("Failed to periodically persist registers cache.")

    def write(self, register: int, value: float, only_on_change: bool = False) -> None:
        """
        Write a float to a Modbus register and update the in-memory cache.
        """
        try:
            value = float(value)
        except Exception:
            logger.warning(f"Cannot cast value {value!r} for register {register}; skipping.")
            return

        if only_on_change:
            prev = self.cache.get(register)
            if prev is not None and abs(prev - value) < 1e-9:
                return

        with self._lock:
            self.server.set_holding_register(register, value, "f")
            self.cache[register] = value

def main():
    """
    1. Loads `setup/mapping.json` to determine register assignments.
    2. Starts the Modbus TCP server.
    3. Restores register values from 'PERSISTEND_PATH/last_registers.csv'.
    4. Starts a 5-minute heartbeat that toggles a dedicated register.
    5. Main Loop:
        - Writes each 'stats:*' Redis hash once to its mapped registers.
        - Continuously mirrors 'health:*' Redis hashes to their mapped registers but only writes when a value changes.
        - Continuously mirrors 'alarm:*' Redis hashes to their mapped registers but only writes when a value changes.
        - Polls selected Docker containers and writes their health codes to mapped registers on change.
    """
    setup_logging(process_name="modbus")
    for additional_libs in (
        "urllib3",
        "urllib3.connectionpool",
        "docker",
        "docker.utils.json_stream",
    ):
        logging.getLogger(additional_libs).setLevel(logging.WARNING)


    redis_db = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=int(os.getenv("REDIS_DB", 0)),
        decode_responses=True
    )

    default_map = {
        "starting": -1,
        "healthy":   0,
        "unhealthy": 1
    }
    
    mapping_path = os.getenv("MAPPING_PATH", "setup/mapping.json")
    logger.info(f"Loading mapping from {mapping_path}.")
    with open(mapping_path) as f:
        MAPPINGS = json.load(f)

    try:
        server = modbus_server.Server(host=MODBUS_HOST, port=MODBUS_PORT)
        server.start()
        logger.info(f"Modbus server started on {MODBUS_HOST}:{MODBUS_PORT}")

        highest_register = max(entry["register"] for entry in MAPPINGS) + 1 

        writer = RegisterWriter(server, persist_interval=10.0)

        for addr in range(0, highest_register + 1, 2):
            val = writer.cache.get(addr, 0.0)
            server.set_holding_register(addr, val, "f") 
        logger.info(f"Prefilled holding registers 0 to {highest_register} from persisted file (0.0 if missing).")

        if not PERSIST_PATH.exists():
            for addr in range(0, highest_register + 1, 2):
                writer.cache.setdefault(addr, 0.0)
            save_persisted(writer.cache)
    except Exception:
        logger.exception("Failed to start Modbus server.")
        exit(1)

    MODBUS_TOGGLE_HEARTBEAT_ADDRESS = 100
    heartbeat_state = False
    def flip_heartbeat():
        nonlocal heartbeat_state
        heartbeat_state = not heartbeat_state
        writer.write(MODBUS_TOGGLE_HEARTBEAT_ADDRESS, float(heartbeat_state), only_on_change=True)
        logger.info(f"Heartbeat to {int(heartbeat_state)} at register {MODBUS_TOGGLE_HEARTBEAT_ADDRESS}.")
        t = threading.Timer(5*60, flip_heartbeat)
        t.daemon = True
        t.start()
    flip_heartbeat()

    logger.debug("Starting Redis to Modbus writer loop...")
    processed = deque(maxlen=500)
    values_previous_iterations = {}
    DOCKER_CHECK_INTERVAL = 5.0  # Sekunden
    last_docker_check = 0.0

    try:
        while True:
            # Health keys, write on change
            for health_keys in redis_db.scan_iter(match="health:*", _type="hash"):

                for entry in MAPPINGS:
                        field = entry["field"]
                        if not field:
                            continue
                        register = entry["register"]

                        val = redis_db.hget(health_keys, field)
                        if val is None:
                            continue

                        try:
                            float_val = float(val.replace(",", "."))
                        except ValueError:
                            logger.warning(f"Cannot parse {val!r} for field '{field}' from {health_keys}.")
                            continue
                        
                        if values_previous_iterations.get(register) != float_val:
                            writer.write(register, float_val, only_on_change=True)
                            logger.debug(f"[health] Wrote {float_val} from {health_keys}.{field} to register {register}.")
                            values_previous_iterations[register] = float_val

            # Alarm keys, write on change
            for alarm_keys in redis_db.scan_iter(match="alarm:*", _type="hash"):
                for entry in MAPPINGS:
                        field = entry["field"]
                        if not field:
                            continue
                        register = entry["register"]

                        val = redis_db.hget(alarm_keys, field)
                        if val is None:
                            continue

                        try:
                            float_val = float(val.replace(",", "."))
                        except ValueError:
                            logger.warning(f"Cannot parse {val!r} for field '{field}' from {alarm_keys}.")
                            continue
                        
                        if values_previous_iterations.get(register) != float_val:
                            writer.write(register, float_val, only_on_change=True)
                            logger.debug(f"[alarm] Wrote {float_val} from {alarm_keys}.{field} to register {register}.")
                            values_previous_iterations[register] = float_val

            # Additonal Container, only check every DOCKER_CHECK_INTERVAL seconds.
            now = time.time()
            if now - last_docker_check >= DOCKER_CHECK_INTERVAL:
                last_docker_check = now

                for svc_name, register in [
                    ("2412511_datapipeline-redis-1", 102),
                ]:
                    try:
                        ctr = docker_client.containers.get(svc_name)
                        attrs = ctr.attrs or {}
                        state = attrs.get("State") or {}
                        health = (state.get("Health") or {}).get("Status")
                        if health is None:
                            health = "healthy" if state.get("Running") else "unhealthy"
                    except Exception:
                        health = "unhealthy"

                    code = default_map.get(health, default_map["unhealthy"])

                    if values_previous_iterations.get(register) != code:
                        logger.debug(f"[additonal]{svc_name}: {health!r} mapped to code {code} in register {register}.")
                        writer.write(register, float(code), only_on_change=True)
                        values_previous_iterations[register] = code

            time.sleep(0.5)

    except Exception:
        logger.exception("Error in modbus writer loop.")

if __name__ == "__main__":
    main()
