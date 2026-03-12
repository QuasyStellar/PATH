#!/usr/bin/env python3
import os
import subprocess
import time
import redis


def log(msg, status="INFO"):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] [{status:4}] {'CLUSTER_SYNC':15} | {msg}", flush=True)


def main():
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        return
    params = {}
    pw = os.getenv("REDIS_PASSWORD")
    if pw:
        params["password"] = pw
    while True:
        try:
            r = redis.from_url(redis_url, **params)
            pubsub = r.pubsub()
            pubsub.subscribe("path:sync")
            log("Started listening for cluster sync signals...")
            break
        except Exception as e:
            log(f"Redis connection failed: {e}. Retrying in 5s...", "WARNING")
            time.sleep(5)

    last_run = 0
    for message in pubsub.listen():
        if message["type"] == "message":
            now = time.time()
            if now - last_run < 5:
                log("Update signal ignored (debounced)")
                continue
            log("Update signal received, triggering process.py...")
            script_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "process.py"
            )
            subprocess.run([script_path])
            last_run = time.time()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
