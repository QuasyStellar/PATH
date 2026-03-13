#!/usr/bin/env -S python3 -u

import asyncio
import os
import time
import subprocess
import redis.asyncio as redis
from pathlib import Path


def log(msg, status="INFO"):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] [{status:4}] {'CLUSTER_SYNC':15} | {msg}", flush=True)


async def main():
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        log("REDIS_URL not set, sync listener disabled", "WARNING")
        return

    pw = os.getenv("REDIS_PASSWORD")
    last_sync = 0
    backoff = 1

    current_dir = Path(__file__).parent.absolute()
    process_script = current_dir / "process.py"

    while True:
        try:
            r = redis.from_url(redis_url, password=pw, decode_responses=True)
            async with r.pubsub() as pubsub:
                await pubsub.subscribe("path:sync")
                log(f"Subscribed to path:sync on {redis_url}")
                backoff = 1

                while True:
                    msg = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=60
                    )
                    if msg and msg["data"] == "reload":
                        now = time.time()
                        if now - last_sync > 5:
                            log("Sync signal received, triggering processing")
                            subprocess.run([str(process_script)], check=False)
                            last_sync = now
                    await asyncio.sleep(0.1)
        except Exception as e:
            log(f"Connection lost: {e}. Retrying in {backoff}s...", "WARNING")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
