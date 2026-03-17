#!/usr/bin/env -S python3 -u

import asyncio
import os
import sys
import time
import redis.asyncio as redis
from pathlib import Path


def log(msg, status="INFO"):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {f'[{status}]':9} {'CLUSTER_SYNC':12} | {msg}", flush=True)


async def main():
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        log("REDIS_URL not set, sync listener disabled", "WARNING")
        return

    pw = os.getenv("REDIS_PASSWORD")
    last_sync = 0
    last_check = 0
    last_hb_check = time.time()
    backoff = 1

    current_dir = Path(__file__).parent.absolute()
    process_script = current_dir / "process.py"
    result_dir = current_dir / "result"
    hash_file = result_dir / ".hash"
    role = os.getenv("NODE_ROLE", "solo").lower()

    running = True

    import signal

    def stop():
        nonlocal running
        running = False
        log("Shutting down...")

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop)

    async def run_sync(reason):
        nonlocal last_sync, last_hb_check
        log(f"Triggering sync: {reason}")
        proc = await asyncio.create_subprocess_exec(
            str(process_script),
            stdout=None,
            stderr=None,
        )
        await proc.wait()
        now = time.time()
        if proc.returncode == 0:
            log("Sync processing completed successfully")
        else:
            log(f"Sync processing failed with exit code {proc.returncode}", "ERROR")
        last_sync = now
        last_hb_check = now

    while running:
        try:
            async with redis.from_url(
                redis_url, password=pw, decode_responses=True
            ) as r:
                async with r.pubsub() as pubsub:
                    await pubsub.subscribe("path:sync")
                    log(f"Subscribed to path:sync on {redis_url}")
                    backoff = 1

                    while running:
                        msg = await pubsub.get_message(
                            ignore_subscribe_messages=True, timeout=1.0
                        )
                        if msg and role != "master":
                            data = msg["data"]
                            if isinstance(data, bytes):
                                data = data.decode()
                            if data == "reload":
                                now = time.time()
                                if now - last_sync > 5:
                                    await run_sync("Pub/Sub reload signal")

                        now = time.time()
                        if role != "master":
                            if now - last_check > 60:
                                last_check = now
                                try:
                                    remote_h = await r.get("path:hash")
                                    last_hb_raw = await r.get("path:last_heartbeat")

                                    should_sync = False
                                    reason = ""

                                    if remote_h:
                                        if isinstance(remote_h, bytes):
                                            remote_h = remote_h.decode()
                                        local_h = (
                                            hash_file.read_text().strip()
                                            if hash_file.exists()
                                            else None
                                        )
                                        if remote_h != local_h:
                                            should_sync = True
                                            reason = "Redis hash changed"

                                    if not should_sync and last_hb_raw:
                                        last_hb = int(
                                            last_hb_raw.decode()
                                            if isinstance(last_hb_raw, bytes)
                                            else last_hb_raw
                                        )
                                        if int(time.time()) - last_hb > 900:
                                            should_sync = True
                                            reason = "Master heartbeat timeout"

                                    if should_sync and now - last_sync > 10:
                                        await run_sync(reason)
                                except Exception:
                                    pass

                        await asyncio.sleep(0.1)
        except Exception as e:
            if not running:
                break
            log(f"Connection lost: {e}. Retrying in {backoff}s...", "WARNING")
            if backoff >= 60:
                log("Critical connection failure, exiting for restart", "ERROR")
                sys.exit(1)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
