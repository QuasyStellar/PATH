#!/usr/bin/env -S python3 -u

import asyncio
import os
import time
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
    last_check = 0
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
                        if msg:
                            data = msg["data"]
                            if isinstance(data, bytes):
                                data = data.decode()

                            if data == "reload":
                                if role == "master":
                                    continue
                                now = time.time()
                                if now - last_sync > 5:
                                    log("Sync signal received, triggering processing")
                                    proc = await asyncio.create_subprocess_exec(
                                        str(process_script),
                                        stdout=asyncio.subprocess.DEVNULL,
                                        stderr=asyncio.subprocess.DEVNULL,
                                    )
                                    await proc.wait()
                                    if proc.returncode == 0:
                                        log("Sync processing completed successfully")
                                    else:
                                        log(
                                            f"Sync processing failed with exit code {proc.returncode}",
                                            "ERROR",
                                        )
                                    last_sync = now
                        now = time.time()
                        if role != "master" and now - last_check > 60:
                            last_check = now
                            try:
                                remote_h = await r.get("path:hash")
                                if remote_h:
                                    local_h = None
                                    if hash_file.exists():
                                        local_h = hash_file.read_text().strip()
                                    if isinstance(remote_h, bytes):
                                        remote_h = remote_h.decode()
                                    if remote_h and remote_h != local_h:
                                        if now - last_sync > 10:
                                            log(
                                                "Redis state changed, triggering processing",
                                                "INFO",
                                            )
                                            proc = await asyncio.create_subprocess_exec(
                                                str(process_script),
                                                stdout=asyncio.subprocess.DEVNULL,
                                                stderr=asyncio.subprocess.DEVNULL,
                                            )
                                            await proc.wait()
                                            if proc.returncode == 0:
                                                log(
                                                    "Sync processing completed successfully"
                                                )
                                            else:
                                                log(
                                                    f"Sync processing failed with exit code {proc.returncode}",
                                                    "ERROR",
                                                )
                                            last_sync = now
                            except Exception:
                                pass
                        await asyncio.sleep(0.1)
        except Exception as e:
            if not running:
                break
            log(f"Connection lost: {e}. Retrying in {backoff}s...", "WARNING")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
