#!/usr/bin/env -S python3 -u

import os
import sys
import time
import hashlib
import ipaddress
import subprocess
import asyncio
import socket
import aiohttp
import zlib
import re
import idna
import shutil
import filecmp
import traceback
from pathlib import Path
from functools import lru_cache

WORKDIR = Path(__file__).parent.absolute()
SOURCE_DIR = WORKDIR / "lists/sources"
MANUAL_DIR = WORKDIR / "lists/manual"
RESULT_DIR = WORKDIR / "result"
DOWNLOAD_DIR = WORKDIR / "download"
TEMP_DIR = DOWNLOAD_DIR / "temp"
KNOT_DIR = Path("/etc/knot-resolver")
LOCK_FILE = WORKDIR / "engine.lock"

CASINO_RE = re.compile(
    r"(casino|[ck]a[szc3][iley1]n[0-9o]|v[uy]l[kc]an|va[vw]ada|most.*bet|leon.*bet|rio.*bet|mel.*bet|ramen.*bet|marathon|max.*bet|bet.*win|gg-*bet|spin.*bet|bet[0-9]|1win|1x|777|slots|poker|jackpot|bonus|winline|fonbet|parimatch|ggbet|banzai|1iks|x.*slot|sloto.*zal|bk.*leon|gold.*fishka|play.*fortuna|dragon.*money|poker.*dom|crypto.*bos|free.*spin|fair.*spin|no.*deposit|igrovye|avtomaty|bookmaker|official|slottica|sykaaa|admiral|pinup|azino|888.*starz|zooma|zenit|eldorado|vodka|newretro|platinum|flagman|arkada|roxb.*|jet.*casino|\.bet$|\.casino$|[^a-z0-9]bet|^bet)",
    re.I,
)

LABEL_RE = re.compile(r"^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$", re.I)


@lru_cache(maxsize=262144)
def _normalize_domain_candidate(line):
    if not line:
        return None
    line = line.strip().lower()
    line = re.split(r"[]_~:/?#\[@!$&'()*+,;=]", line)[0]
    line = line.strip(".")
    if not line:
        return None
    if not all(ord(c) < 128 for c in line):
        try:
            line = idna.encode(line).decode("ascii")
        except Exception:
            ascii_only = "".join(c for c in line if ord(c) < 128)
            if not ascii_only:
                return None
            line = ascii_only.strip(".")
    return line


def log(phase, msg, status="INFO"):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {f'[{status}]':9} {phase:12} | {msg}", flush=True)


def validate_domain(line):
    line = _normalize_domain_candidate(line)
    if not line:
        return None
    if "." not in line or len(line) > 253:
        return None
    labels = line.split(".")
    for label in labels:
        if not label or not LABEL_RE.match(label):
            return None
    return line


@lru_cache(maxsize=262144)
def parse_adblock_line(line):
    if not line:
        return None
    line = line.strip()
    if not line or line.startswith("!") or line.startswith("["):
        return None
    if "##" in line or "#@#" in line:
        return None

    if "*" in line:
        return None

    is_ex = False
    domain = None

    if line.startswith("@@||"):
        is_ex = True
        domain = line[4:].split("^")[0]
    elif line.startswith("||"):
        is_ex = False
        domain = line[2:].split("^")[0]
    else:
        is_ex = line.startswith("@@")
        if is_ex:
            line = line[2:]
        domain = re.split(r"[]_~:/?#\[@!$&'()*+,;=]", line)[0]

    if not domain:
        return None
    
    v = validate_domain(domain)
    if not v:
        return None
    return (v, is_ex)


def validate_file(path, is_ip, f_cas):
    res, cas_count, adblock_rules = set(), 0, set()
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return (res if is_ip else adblock_rules), 0
    try:
        with open(path, "rb") as f:
            for line_bytes in f:
                try:
                    line = line_bytes.decode("utf-8", errors="replace").strip()
                except Exception:
                    continue
                if not line or line[0] in "!#[]":
                    continue

                if is_ip:
                    ip_part = line.partition("#")[0].strip()
                    if ip_part:
                        try:
                            ipaddress.ip_network(ip_part, strict=False)
                            res.add(ip_part)
                        except Exception:
                            pass
                else:
                    parsed = parse_adblock_line(line)
                    if not parsed:
                        continue
                    v, is_ex = parsed
                    if f_cas and CASINO_RE.search(v):
                        cas_count += 1
                        continue
                    adblock_rules.add((v, is_ex))
    except Exception:
        pass
    return (res if is_ip else adblock_rules), cas_count


def optimize_trie(domains):
    if not domains:
        return []
    sorted_domains = sorted(domains, key=len)
    trie = {}
    for d in sorted_domains:
        parts = d.split(".")[::-1]
        curr = trie
        is_redundant = False
        for p in parts:
            if "__root__" in curr:
                is_redundant = True
                break
            if p not in curr:
                curr[p] = {}
            curr = curr[p]
        if not is_redundant:
            curr["__root__"] = True

    res = []
    stack = [(trie, [])]
    while stack:
        curr, path = stack.pop()
        if "__root__" in curr:
            res.append(".".join(path[::-1]))
            continue
        for p, next_node in curr.items():
            stack.append((next_node, path + [p]))
    return res


def sub_nets_optimized(inc_nets, exc_ips):
    if not inc_nets:
        return []
    if not exc_ips:
        return sorted(inc_nets)
    ranges = []
    for net in inc_nets:
        ranges.append((int(net.network_address), int(net.broadcast_address)))
    exc_ranges = []
    for net in exc_ips:
        exc_ranges.append((int(net.network_address), int(net.broadcast_address)))
    ranges.sort()
    exc_ranges.sort()
    result_ranges = []
    for r_start, r_end in ranges:
        curr_start = r_start
        for e_start, e_end in exc_ranges:
            if e_start > r_end or e_end < curr_start:
                continue
            if e_start > curr_start:
                result_ranges.append((curr_start, e_start - 1))
            curr_start = max(curr_start, e_end + 1)
            if curr_start > r_end:
                break
        if curr_start <= r_end:
            result_ranges.append((curr_start, r_end))
    final_nets = []
    for s, e in result_ranges:
        final_nets.extend(
            ipaddress.summarize_address_range(
                ipaddress.ip_address(s), ipaddress.ip_address(e)
            )
        )
    return final_nets


class Processor:
    def __init__(self, env):
        self.env = env
        for d in [RESULT_DIR, DOWNLOAD_DIR]:
            d.mkdir(parents=True, exist_ok=True)
        self.r = None
        if env.get("REDIS_URL"):
            import redis

            try:
                self.r = redis.from_url(
                    env["REDIS_URL"],
                    password=env.get("REDIS_PASSWORD"),
                    decode_responses=False,
                )
            except Exception:
                pass

    def _r_exists(self, key):
        if not self.r:
            return 0
        return self.r.exists(key)

    def _r_get(self, key):
        if not self.r:
            return None
        return self.r.get(key)

    def _r_set(self, *args, **kwargs):
        if not self.r:
            return False
        return self.r.set(*args, **kwargs)

    async def r_exists(self, key):
        return await asyncio.to_thread(self._r_exists, key)

    async def r_get(self, key):
        return await asyncio.to_thread(self._r_get, key)

    async def r_set(self, *args, **kwargs):
        return await asyncio.to_thread(self._r_set, *args, **kwargs)

    def get_state_hash(self):
        h = hashlib.md5(usedforsecurity=False)
        for p in [SOURCE_DIR, MANUAL_DIR, DOWNLOAD_DIR]:
            if not p.exists():
                continue
            for f in sorted(p.rglob("*.txt")):
                if f.is_relative_to(TEMP_DIR):
                    continue
                if f.is_file() and f.stat().st_size > 0:
                    rel_path = f.relative_to(WORKDIR)
                    h.update(f"{rel_path}".encode())
                    try:
                        h.update(f.read_bytes())
                    except Exception:
                        pass

        for k in [
            "NODE_ROLE",
            "ROUTE_ALL",
            "BLOCK_ADS",
            "FILTER_CASINO",
            "ENABLE_IPV6",
            "AGGREGATE_COUNT",
            "PATH_DNS",
            "PUBLIC_DNS",
            "IP",
            "EXTERNAL_IP",
            "FAKE_IP",
            "FAKE_NETMASK_V4",
            "FAKE_IP6",
            "FAKE_NETMASK_V6",
            "DOH_ENABLE",
        ]:
            h.update(f"{k}={self.env.get(k, '')}".encode())
        return h.hexdigest()

    async def update_sources(self):
        usage = shutil.disk_usage(WORKDIR)
        if usage.free < 50 * 1024 * 1024:
            log("FETCHER", "Not enough disk space to update sources!", "ERROR")
            return

        url_map = {}
        for f in SOURCE_DIR.glob("*.txt"):
            with open(f) as f_in:
                for line in f_in:
                    u = line.strip()
                    if u.startswith("http"):
                        if u not in url_map:
                            url_map[u] = []
                        url_map[u].append(
                            TEMP_DIR
                            / "downloads"
                            / f.stem
                            / f"{hashlib.md5(u.encode(), usedforsecurity=False).hexdigest()[:8]}.txt"
                        )

        if not url_map:
            return

        temp_download_dir = TEMP_DIR / "downloads"
        if temp_download_dir.exists():
            await asyncio.to_thread(shutil.rmtree, temp_download_dir)
        await asyncio.to_thread(temp_download_dir.mkdir, parents=True)

        log("FETCHER", f"Checking updates for {len(url_map)} sources...")
        sem = asyncio.Semaphore(10)
        async with aiohttp.ClientSession() as session:
            tasks = [
                self.fetch(session, url, paths[0], sem)
                for url, paths in url_map.items()
            ]
            results = await asyncio.gather(*tasks)

            for (url, paths), success in zip(url_map.items(), results):
                if success:
                    main_path = paths[0]
                    for extra_path in paths[1:]:
                        await asyncio.to_thread(
                            extra_path.parent.mkdir, parents=True, exist_ok=True
                        )
                        await asyncio.to_thread(shutil.copy, main_path, extra_path)

        if temp_download_dir.exists():
            updated_count = 0
            for new_dir in temp_download_dir.iterdir():
                if new_dir.is_dir():
                    new_files = list(new_dir.glob("*.txt"))
                    if not new_files:
                        continue

                    dest_dir = DOWNLOAD_DIR / new_dir.name
                    if dest_dir.exists():
                        await asyncio.to_thread(shutil.rmtree, dest_dir)
                    await asyncio.to_thread(shutil.move, str(new_dir), str(dest_dir))
                    updated_count += 1

            if updated_count > 0:
                log(
                    "FETCHER",
                    f"Successfully updated {updated_count} source directories",
                )

        if TEMP_DIR.exists():
            await asyncio.to_thread(shutil.rmtree, TEMP_DIR)

    async def fetch(self, session, url, path, sem):
        async with sem:
            try:
                await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
                async with session.get(url, timeout=30) as r:
                    if r.status == 200:
                        data = await r.read()
                        if url.endswith(".gz"):
                            data = zlib.decompress(data, 16 + zlib.MAX_WBITS)

                        if len(data) < 10 or b"<html" in data[:512].lower():
                            log(
                                "FETCHER",
                                f"Invalid data from {url} (too small or HTML)",
                                "WARNING",
                            )
                            return False

                        if path.exists():
                            old_data = await asyncio.to_thread(path.read_bytes)
                            if (
                                hashlib.md5(data, usedforsecurity=False).hexdigest()
                                == hashlib.md5(
                                    old_data, usedforsecurity=False
                                ).hexdigest()
                            ):
                                return True

                        await asyncio.to_thread(
                            path.parent.mkdir, parents=True, exist_ok=True
                        )
                        await asyncio.to_thread(path.write_bytes, data)

                        return True
                    else:
                        log(
                            "FETCHER",
                            f"Failed to download {url}: HTTP {r.status}",
                            "WARNING",
                        )
            except Exception as e:
                log("FETCHER", f"Error downloading {url}: {e}", "WARNING")
        return False

    def load(self, names, is_ip=False):
        res, cas_total = set(), 0
        f_cas = self.env.get("FILTER_CASINO") == "y"
        f_ads = self.env.get("BLOCK_ADS") == "y"
        for name in names:
            if name == "include-adblock-hosts" and not f_ads:
                continue
            files = []
            d_path = DOWNLOAD_DIR / name
            if d_path.exists() and d_path.is_dir():
                files.extend(d_path.glob("*.txt"))
            m_path = MANUAL_DIR / f"{name}.txt"
            if m_path.exists():
                files.append(m_path)

            for f in files:
                out, cas = validate_file(f, is_ip, f_cas)
                res.update(out)
                cas_total += cas
        return res, cas_total

    def aggregate(self, nets, limit, ver=4):
        if not nets:
            return []
        res = list(ipaddress.collapse_addresses(nets))
        if limit > 0 and len(res) > limit:
            target = 24 if ver == 4 else 64
            res = list(
                ipaddress.collapse_addresses(
                    [
                        n.supernet(new_prefix=target) if n.prefixlen > target else n
                        for n in res
                    ]
                )
            )
            while len(res) > limit:
                mp = max(n.prefixlen for n in res)
                if mp <= (12 if ver == 4 else 32):
                    break
                res = list(
                    ipaddress.collapse_addresses(
                        [n.supernet() if n.prefixlen == mp else n for n in res]
                    )
                )
        return sorted(res)

    async def sync_to_knot(self):
        log("SYNC", "Syncing RPZ zones to DNS server...")
        changed = False
        for z in ["deny", "deny2", "proxy"]:
            src, dst = RESULT_DIR / f"{z}.rpz", KNOT_DIR / f"{z}.rpz"

            src_exists = await asyncio.to_thread(src.exists)
            if src_exists:
                dst_exists = await asyncio.to_thread(dst.exists)
                is_same = False
                if dst_exists:
                    is_same = await asyncio.to_thread(
                        filecmp.cmp, src, dst, shallow=False
                    )

                if not dst_exists or not is_same:
                    tmp_dst = dst.with_suffix(".tmp")
                    try:
                        await asyncio.to_thread(shutil.copy2, src, tmp_dst)
                        await asyncio.to_thread(os.chmod, tmp_dst, 0o644)
                        await asyncio.to_thread(tmp_dst.rename, dst)
                        changed = True
                    except Exception as e:
                        log(
                            "SYNC",
                            f"Failed to atomic copy {src} to {dst}: {e}",
                            "ERROR",
                        )
        if changed:
            ctrl_dir = "/run/knot-resolver/control"
            ctrl_exists = await asyncio.to_thread(os.path.exists, ctrl_dir)
            if ctrl_exists:
                for s_name in await asyncio.to_thread(os.listdir, ctrl_dir):
                    s_path = os.path.join(ctrl_dir, s_name)
                    try:
                        await asyncio.to_thread(
                            subprocess.run,
                            ["socat", "-", f"unix-connect:{s_path}"],
                            input=b"cache.clear()\n",
                            capture_output=True,
                            timeout=5,
                        )
                    except subprocess.TimeoutExpired:
                        log(
                            "SYNC", f"Knot cache clear timed out on {s_name}", "WARNING"
                        )
                    except Exception as e:
                        log(
                            "SYNC",
                            f"Failed to clear Knot cache on {s_name}: {e}",
                            "WARNING",
                        )

    def _sync_to_redis_blocking(self, h):
        if not self.r:
            return False

        role = self.env.get("NODE_ROLE", "solo").lower()
        my_id = socket.gethostname()

        try:
            current_master = self.r.get("path:master_lock")
            if current_master:
                if isinstance(current_master, bytes):
                    current_master = current_master.decode()

                if current_master != my_id:
                    if role == "master":
                        log(
                            "REDIS",
                            f"Reclaiming Master role from {current_master}...",
                            "INFO",
                        )
                        self.r.set("path:master_lock", my_id, ex=3600)
                    else:
                        log(
                            "REDIS",
                            f"Conflict: Node {current_master} is now Master. Aborting sync.",
                            "WARNING",
                        )
                        return False
        except Exception as e:
            log("REDIS", f"Master lock check failed: {e}", "WARNING")

        log("REDIS", "Pushing results to cluster storage...")
        try:
            pipe = self.r.pipeline()
            for f in [
                "proxy.rpz",
                "deny.rpz",
                "deny2.rpz",
                "route-ips.txt",
                "route-ips-v6.txt",
            ]:
                p = RESULT_DIR / f
                if p.exists():
                    pipe.set(f"path:data:{f}", zlib.compress(p.read_bytes()))
            pipe.set("path:hash", h)
            pipe.set("path:last_heartbeat", int(time.time()))
            lists_hash = hashlib.md5(usedforsecurity=False)
            for dir_path in [SOURCE_DIR, MANUAL_DIR]:
                if dir_path.exists():
                    for f in sorted(dir_path.glob("*.txt")):
                        try:
                            lists_hash.update(f.name.encode())
                            lists_hash.update(f.read_bytes())
                        except Exception:
                            continue
            new_lists_hash = lists_hash.hexdigest()
            remote_lists_hash = self.r.get("path:lists:hash")
            if isinstance(remote_lists_hash, bytes):
                remote_lists_hash = remote_lists_hash.decode()
            lists_ready = bool(self.r.get("path:lists:ready"))

            if new_lists_hash != remote_lists_hash or not lists_ready:
                changed_sources = 0
                changed_manual = 0
                for name, dir_path in [("sources", SOURCE_DIR), ("manual", MANUAL_DIR)]:
                    key = f"path:lists:{name}"
                    files = sorted(dir_path.glob("*.txt")) if dir_path.exists() else []
                    new_names = {f.name for f in files}
                    try:
                        existing = set(self.r.hkeys(key))
                    except Exception:
                        existing = set()
                    to_delete = [n for n in existing if n not in new_names]
                    if to_delete:
                        pipe.hdel(key, *to_delete)
                    for f in files:
                        try:
                            data = zlib.compress(f.read_bytes())
                            pipe.hset(key, f.name, data)
                            if name == "sources":
                                changed_sources += 1
                            else:
                                changed_manual += 1
                        except Exception:
                            continue
                pipe.set("path:lists:hash", new_lists_hash)
                pipe.set("path:lists:ready", "1")
                log(
                    "REDIS",
                    f"Synced lists to Redis (sources={changed_sources}, manual={changed_manual})",
                )
            pipe.publish("path:sync", "reload")
            pipe.execute()
            return True
        except Exception as e:
            log("REDIS", f"Sync failed: {e}", "ERROR")
            return False

    async def sync_to_redis(self, h):
        return await asyncio.to_thread(self._sync_to_redis_blocking, h)

    def _sync_from_redis_blocking(self):
        if not self.r:
            return False
        try:
            remote_h = self.r.get("path:hash")
            if not remote_h:
                return False
            if isinstance(remote_h, bytes):
                remote_h = remote_h.decode()

            h_file = RESULT_DIR / ".hash"
            if h_file.exists() and h_file.read_text().strip() == remote_h:
                return "NO_CHANGE"

            log("REDIS", "Fetching data from cluster master...")
            all_ok = True
            lists_ready = bool(self.r.get("path:lists:ready"))
            if lists_ready:
                for name, dir_path in [("sources", SOURCE_DIR), ("manual", MANUAL_DIR)]:
                    key = f"path:lists:{name}"
                    try:
                        data_map = self.r.hgetall(key)
                    except Exception:
                        data_map = {}
                    if data_map:
                        dir_path.mkdir(parents=True, exist_ok=True)
                        keep = set()
                        for fname, data in data_map.items():
                            try:
                                if isinstance(fname, bytes):
                                    fname = fname.decode()
                                keep.add(fname)
                                if isinstance(data, memoryview):
                                    data = data.tobytes()
                                if isinstance(data, bytes):
                                    content = zlib.decompress(data)
                                else:
                                    content = zlib.decompress(bytes(data))

                                target = dir_path / fname
                                tmp_target = target.with_suffix(".tmp")
                                tmp_target.write_bytes(content)
                                tmp_target.rename(target)
                            except Exception:
                                all_ok = False
                        for f in dir_path.glob("*.txt"):
                            if f.name not in keep:
                                try:
                                    f.unlink()
                                except Exception:
                                    pass

            for f in [
                "proxy.rpz",
                "deny.rpz",
                "deny2.rpz",
                "route-ips.txt",
                "route-ips-v6.txt",
            ]:
                data = self.r.get(f"path:data:{f}")
                if data:
                    try:
                        target = RESULT_DIR / f
                        tmp_target = target.with_suffix(".tmp")
                        tmp_target.write_bytes(zlib.decompress(data))
                        tmp_target.rename(target)
                    except Exception:
                        all_ok = False
                else:
                    all_ok = False

            if all_ok:
                target_h = RESULT_DIR / ".hash"
                tmp_h = target_h.with_suffix(".tmp")
                tmp_h.write_text(
                    remote_h.decode() if isinstance(remote_h, bytes) else remote_h
                )
                tmp_h.rename(target_h)
                return True
            return False

        except Exception:
            return False

    async def sync_from_redis(self):
        res = await asyncio.to_thread(self._sync_from_redis_blocking)
        if res == "NO_CHANGE":
            return True
        if res:
            await self.sync_to_knot()
            return True
        return False

    async def run(self):
        try:
            role = self.env.get("NODE_ROLE", "solo").lower()
            my_id = socket.gethostname()
            is_master = role != "worker"

            if self.r:
                last_hb_raw = await self.r_get("path:last_heartbeat")
                if last_hb_raw:
                    last_hb = int(
                        last_hb_raw.decode()
                        if isinstance(last_hb_raw, bytes)
                        else last_hb_raw
                    )
                    if int(time.time()) - last_hb > 900:
                        log(
                            "ENGINE",
                            "Master heartbeat timed out. Attempting failover...",
                            "WARNING",
                        )
                        lock = await self.r_set(
                            "path:master_lock", my_id, nx=True, ex=3600
                        )
                        if lock:
                            log(
                                "ENGINE",
                                "I am the temporary Master now (Failover active).",
                                "INFO",
                            )
                            is_master = True
                        else:
                            current_m = await self.r_get("path:master_lock")
                            if (
                                current_m
                                and (
                                    current_m.decode()
                                    if isinstance(current_m, bytes)
                                    else current_m
                                )
                                == my_id
                            ):
                                is_master = True
                            else:
                                log(
                                    "ENGINE",
                                    "Another node is already handling failover.",
                                    "INFO",
                                )
                                if role == "worker":
                                    return

            if role == "worker" and not is_master:
                if await self.sync_from_redis():
                    log("ENGINE", "Worker sync completed")
                else:
                    log(
                        "ENGINE",
                        "Worker sync failed, continuing to baseline initialization",
                        "WARNING",
                    )

            if is_master:
                await self.update_sources()

            new_h = self.get_state_hash()
            h_file = RESULT_DIR / ".hash"
            if h_file.exists() and h_file.read_text() == new_h:
                if all(
                    (RESULT_DIR / f).exists() and (RESULT_DIR / f).stat().st_size > 0
                    for f in [
                        "proxy.rpz",
                        "deny.rpz",
                        "deny2.rpz",
                        "route-ips.txt",
                        "route-ips-v6.txt",
                    ]
                ):
                    log("ENGINE", "No changes detected, skipping generation")
                    await self.sync_to_knot()
                    if self.r:
                        await self.sync_to_redis(new_h)
                    return

            log("ENGINE", "Processing started")
            in_ips_raw, _ = self.load(["include-ips"], is_ip=True)
            ex_ips_raw, _ = self.load(["exclude-ips"], is_ip=True)

            i_v4 = [ipaddress.ip_network(i, False) for i in in_ips_raw if ":" not in i]
            i_v6 = [ipaddress.ip_network(i, False) for i in in_ips_raw if ":" in i]
            e_v4 = [ipaddress.ip_network(i, False) for i in ex_ips_raw if ":" not in i]
            e_v6 = [ipaddress.ip_network(i, False) for i in ex_ips_raw if ":" in i]

            limit = int(self.env.get("AGGREGATE_COUNT", 500))
            final_v4 = sub_nets_optimized(self.aggregate(i_v4, limit, 4), e_v4)
            final_v6 = sub_nets_optimized(self.aggregate(i_v6, limit, 6), e_v6)

            for fname, nets in [
                ("route-ips.txt", final_v4),
                ("route-ips-v6.txt", final_v6),
            ]:
                target = RESULT_DIR / fname
                tmp_target = target.with_suffix(".tmp")
                await asyncio.to_thread(
                    tmp_target.write_text, "\n".join(map(str, nets))
                )
                await asyncio.to_thread(tmp_target.rename, target)

            hosts_proxy_raw, c1 = self.load(["include-hosts", "rpz"])
            hosts_ad_raw, c2 = self.load(["include-adblock-hosts"])
            hosts_deny2_raw, _ = self.load(["rpz2"])

            ex_proxy_only, _ = self.load(["exclude-hosts"])
            ex_ad_only, _ = self.load(["exclude-adblock-hosts"])
            ex_global, c3 = self.load(["remove-hosts"])
            cas_total = c1 + c2 + c3

            ex_common = {d for d, ex in ex_global}
            ex_proxy = (
                ex_common
                | {d for d, ex in ex_proxy_only}
                | {d for d, ex in hosts_proxy_raw if ex}
            )
            ex_ad = (
                ex_common
                | {d for d, ex in ex_ad_only}
                | {d for d, ex in hosts_ad_raw if ex}
            )
            ex_deny2 = ex_common | {d for d, ex in hosts_deny2_raw if ex}

            def strip_prefixes(domains):
                res = set()
                prefix_re = re.compile(
                    r"^([0-9]*www[0-9]*|hd[0-9]*|[A-Za-z]|[0-9]+)\.", re.I
                )
                for d in domains:
                    if d.count(".") >= 2:
                        res.add(prefix_re.sub("", d))
                    else:
                        res.add(d)
                return res

            proxy_raw = {d for d, ex in hosts_proxy_raw if not ex}
            adblock_raw = {d for d, ex in hosts_ad_raw if not ex}
            deny2_raw = {d for d, ex in hosts_deny2_raw if not ex}

            proxy_raw = strip_prefixes(proxy_raw)
            adblock_raw = strip_prefixes(adblock_raw)
            deny2_raw = strip_prefixes(deny2_raw)

            proxy_domains = optimize_trie(proxy_raw - ex_proxy)
            adblock_domains = sorted(list(adblock_raw - ex_ad))
            deny2_domains = sorted(list(deny2_raw - ex_deny2))

            if self.env.get("ROUTE_ALL") == "y":
                proxy_domains = ["."]

            async def write_rpz(name, domains, excluded_domains=None, ra=False):
                out_path = RESULT_DIR / f"{name}.rpz"
                tmp_path = out_path.with_suffix(".tmp")

                def _write():
                    with open(tmp_path, "w", encoding="utf-8") as f_out:
                        f_out.write("$TTL 10800\n@ SOA . . (1 1 1 1 10800)\n")
                        if ra and name == "proxy":
                            f_out.write("* CNAME .\n")

                        if excluded_domains:
                            for d in sorted(excluded_domains):
                                f_out.write(f"{d}. CNAME rpz-passthru.\n")
                                f_out.write(f"*.{d}. CNAME rpz-passthru.\n")

                        for d in sorted(domains):
                            if d == ".":
                                continue
                            f_out.write(f"{d}. CNAME .\n")
                            f_out.write(f"*.{d}. CNAME .\n")

                await asyncio.to_thread(_write)
                await asyncio.to_thread(tmp_path.rename, out_path)

            await write_rpz(
                "proxy", proxy_domains, ex_proxy, self.env.get("ROUTE_ALL") == "y"
            )
            await write_rpz("deny", adblock_domains, ex_ad)
            await write_rpz("deny2", deny2_domains, ex_deny2)

            h_file_tmp = h_file.with_suffix(".tmp")
            await asyncio.to_thread(h_file_tmp.write_text, new_h)
            await asyncio.to_thread(h_file_tmp.rename, h_file)

            await self.sync_to_knot()
            if self.r:
                await self.sync_to_redis(new_h)

            log("ENGINE", "=============================================")
            log("ENGINE", f" IPv4 Routes:    {len(final_v4)}")
            log("ENGINE", f" IPv6 Routes:    {len(final_v6)}")
            log("ENGINE", f" Blocked:        {len(adblock_domains)}")
            log("ENGINE", f" Proxied:        {len(proxy_domains)}")
            log("ENGINE", f" Casino Clean:   {cas_total}")
            log("ENGINE", "=============================================")
            log("ENGINE", "Status: SUCCESS")
        except Exception:
            log("ENGINE", f"CRITICAL CRASH: {traceback.format_exc()}", "ERROR")
            sys.exit(1)
        finally:
            if self.r:
                try:
                    self.r.close()
                except Exception:
                    pass


if __name__ == "__main__":
    import fcntl

    lock_file = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_file.write(str(os.getpid()))
        lock_file.flush()
    except OSError:
        log("ENGINE", "Another instance is already running", "WARNING")
        sys.exit(0)

    try:
        env_file = WORKDIR / ".env"
        env = {}
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if "=" in line:
                    k, v = line.strip().split("=", 1)
                    env[k] = v
        asyncio.run(Processor(env).run())
    finally:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_UN)
            lock_file.close()
            if LOCK_FILE.exists():
                LOCK_FILE.unlink()
        except Exception:
            pass
