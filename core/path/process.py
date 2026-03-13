#!/usr/bin/env -S python3 -u

import os
import sys
import time
import hashlib
import ipaddress
import subprocess
import asyncio
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
KNOT_DIR = Path("/etc/knot-resolver")
LOCK_FILE = Path("/tmp/path_engine.lock")

PROXY_URL = "https://api.codetabs.com/v1/proxy?quest="

CASINO_RE = re.compile(
    r"([ck]a[szc3][iley1]n[0-9o]|vulkan|vlk|v[uy]l[kc]an|va[vw]ada|x.*bet|most.*bet|leon"
    r".*bet|rio.*bet|mel.*bet|ramen.*bet|marathon|max.*bet|bet.*win|gg-*bet|spin.*bet"
    r"|banzai|1iks|x.*slot|sloto.*zal|bk.*leon|gold.*fishka|play.*fortuna|dragon.*money"
    r"|poker.*dom|1.*win|crypto.*bos|free.*spin|fair.*spin|no.*deposit|igrovye|avtomaty"
    r"|bookmaker|zerkalo|official|slottica|sykaaa|admiral|pinup|pari.*match|betting|"
    r"partypoker|jackpot|bonus|azino|888.*starz|zooma|zenit|eldorado|slots|vodka|"
    r"newretro|platinum|flagman|arkada|game.*top|vavada|joy.*casino|sol.*casino|"
    r"roxb.*|riobet|fresh.*cas|izzy.*cas|legzo.*cas|volna.*cas|starda.*cas|drip.*cas|"
    r"\.bet$|\.casino$)",
    re.I,
)

LABEL_RE = re.compile(r"^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$", re.I)


def log(phase, msg, status="INFO"):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] [{status:4}] {phase:15} | {msg}", flush=True)


@lru_cache(maxsize=262144)
def validate_domain(line):
    if not line:
        return None
    line = line.strip().lower().strip(".")
    if not all(ord(c) < 128 for c in line):
        try:
            line = idna.encode(line).decode("ascii")
        except Exception:
            return None
    labels = line.split(".")
    if len(labels) < 2 or len(line) > 253:
        return None
    for label in labels:
        if not LABEL_RE.match(label):
            return None
    return line


def validate_file(path, is_ip, f_cas):
    res, cas_count, adblock_rules = set(), 0, set()
    try:
        with open(path, "r", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("!") or line.startswith("["):
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
                    if "##" in line or "#@#" in line:
                        continue

                    is_ex = line.startswith("@@")
                    if is_ex:
                        line = line[2:]

                    if line.startswith("||"):
                        line = line[2:]

                    line = re.split(r"[\^\$/\s#]", line)[0]

                    if not line:
                        continue
                    if f_cas and CASINO_RE.search(line):
                        cas_count += 1
                        continue
                    v = validate_domain(line)
                    if v:
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

    def walk(curr, path):
        if "__root__" in curr:
            d = ".".join(path[::-1])
            res.append(d)
            return
        for p, next_node in curr.items():
            path.append(p)
            walk(next_node, path)
            path.pop()

    walk(trie, [])
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
        self.stats = {"casino": 0, "v4": 0, "v6": 0, "proxy": 0}

    def get_state_hash(self):
        h = hashlib.md5()
        for p in [SOURCE_DIR, MANUAL_DIR, DOWNLOAD_DIR]:
            if not p.exists():
                continue
            for f in sorted(p.rglob("*.txt")):
                stat = f.stat()
                rel_path = f.relative_to(WORKDIR)
                h.update(f"{rel_path}:{stat.st_mtime}:{stat.st_size}".encode())
        for k in [
            "NODE_ROLE",
            "ROUTE_ALL",
            "BLOCK_ADS",
            "FILTER_CASINO",
            "ENABLE_IPV6",
            "AGGREGATE_COUNT",
        ]:
            h.update(f"{k}={self.env.get(k, '')}".encode())
        return h.hexdigest()

    async def update_sources(self):
        urls_to_files = {}
        for f in SOURCE_DIR.glob("*.txt"):
            with open(f, "r") as f_in:
                for line in f_in:
                    u = line.strip()
                    if u.startswith("http"):
                        dest = (
                            DOWNLOAD_DIR
                            / f.stem
                            / f"{hashlib.md5(u.encode()).hexdigest()[:8]}.txt"
                        )
                        urls_to_files[u] = dest
        if not urls_to_files:
            return
        log("FETCHER", f"Updating {len(urls_to_files)} unique sources...")
        sem = asyncio.Semaphore(20)
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch(session, u, p, sem) for u, p in urls_to_files.items()]
            results = await asyncio.gather(*tasks)
        log("FETCHER", f"Fetched {len(urls_to_files)} URLs, {sum(results)} successful.")

    async def fetch(self, session, url, path, sem):
        async with sem:
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                async with session.get(url, timeout=30) as r:
                    if r.status == 200:
                        data = await r.read()
                        if url.endswith(".gz"):
                            data = zlib.decompress(data, 16 + zlib.MAX_WBITS)
                        if len(data) < 10 or b"<html" in data[:512].lower():
                            return False

                        if path.exists():
                            with open(path, "rb") as f_old:
                                if (
                                    hashlib.md5(data).hexdigest()
                                    == hashlib.md5(f_old.read()).hexdigest()
                                ):
                                    return True

                        path.write_bytes(data)
                        return True
            except Exception:
                pass
        return False

    def load(self, names, is_ip=False):
        res, cas_total = set(), 0
        f_cas = self.env.get("FILTER_CASINO") == "y"
        for name in names:
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

    def sync_to_knot(self):
        log("SYNC", "Syncing RPZ zones to DNS server...")
        changed = False
        for z in ["deny", "deny2", "proxy"]:
            src, dst = RESULT_DIR / f"{z}.rpz", KNOT_DIR / f"{z}.rpz"
            if src.exists():
                if not dst.exists() or not filecmp.cmp(src, dst, shallow=False):
                    shutil.copy2(src, dst)
                    changed = True
        if changed:
            ctrl_dir = "/run/knot-resolver/control"
            if os.path.exists(ctrl_dir):
                for s_name in os.listdir(ctrl_dir):
                    s_path = os.path.join(ctrl_dir, s_name)
                    subprocess.run(
                        ["socat", "-", f"unix-connect:{s_path}"],
                        input=b"cache.clear()\n",
                        capture_output=True,
                    )

    def sync_to_redis(self, h):
        if not self.r:
            return
        log("REDIS", "Pushing results to cluster storage...")
        try:
            pipe = self.r.pipeline()
            for f in ["proxy.rpz", "deny.rpz", "deny2.rpz", "ips_v4.txt", "ips_v6.txt"]:
                p = RESULT_DIR / f
                if p.exists():
                    pipe.set(f"path:data:{f}", zlib.compress(p.read_bytes()))
            pipe.set("path:hash", h)
            pipe.publish("path:sync", "reload")
            pipe.execute()
        except Exception as e:
            log("REDIS", f"Sync failed: {e}", "ERROR")

    def sync_from_redis(self):
        if not self.r:
            return False
        try:
            log("REDIS", "Fetching data from cluster master...")
            remote_h = self.r.get("path:hash")
            if not remote_h:
                return False
            for f in ["proxy.rpz", "deny.rpz", "deny2.rpz", "ips_v4.txt", "ips_v6.txt"]:
                data = self.r.get(f"path:data:{f}")
                if data:
                    (RESULT_DIR / f).write_bytes(zlib.decompress(data))
            (RESULT_DIR / ".hash").write_text(remote_h.decode())
            self.sync_to_knot()
            return True
        except Exception:
            return False

    async def run(self):
        try:
            role = self.env.get("NODE_ROLE", "solo").lower()
            if role == "worker" and self.sync_from_redis():
                log("ENGINE", "Worker sync completed")
                return

            await self.update_sources()
            new_h = self.get_state_hash()
            h_file = RESULT_DIR / ".hash"
            if h_file.exists() and h_file.read_text() == new_h:
                if all(
                    (RESULT_DIR / f).exists() and (RESULT_DIR / f).stat().st_size > 0
                    for f in ["proxy.rpz", "deny.rpz"]
                ):
                    log("ENGINE", "No changes detected, skipping generation")
                    self.sync_to_knot()
                    if self.r:
                        self.sync_to_redis(new_h)
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

            (RESULT_DIR / "ips_v4.txt").write_text("\n".join(map(str, final_v4)))
            (RESULT_DIR / "ips_v6.txt").write_text("\n".join(map(str, final_v6)))

            hosts_inc, cas_total = self.load(
                ["include-hosts", "include-adblock-hosts", "rpz", "rpz2"]
            )
            hosts_ex, _ = self.load(
                ["exclude-hosts", "exclude-adblock-hosts", "remove-hosts"]
            )

            in_ad = {d for d, ex in hosts_inc if not ex}
            ex_ad = {d for d, ex in hosts_inc if ex} | {d for d, ex in hosts_ex}

            proxy_domains = optimize_trie(in_ad - ex_ad)
            if self.env.get("ROUTE_ALL") == "y":
                proxy_domains = ["."]

            def write_rpz(name, domains, ra=False):
                lines = ["$TTL 10800", "@ SOA . . (1 1 1 1 10800)"]
                if ra and name == "proxy":
                    lines.append("* CNAME .")
                for d in sorted(domains):
                    if d != ".":
                        lines.extend([f"{d}. CNAME .", f"*.{d}. CNAME ."])
                (RESULT_DIR / f"{name}.rpz").write_text("\n".join(lines) + "\n")

            write_rpz("proxy", proxy_domains, self.env.get("ROUTE_ALL") == "y")
            write_rpz("deny", optimize_trie({d for d, ex in hosts_ex}))

            rpz2_data, _ = self.load(["rpz2"])
            write_rpz("deny2", optimize_trie({d for d, ex in rpz2_data}))

            h_file.write_text(new_h)
            self.sync_to_knot()
            if self.r:
                self.sync_to_redis(new_h)

            log("ENGINE", "=============================================")
            log("ENGINE", f" IPv4 Routes:    {len(final_v4)}")
            log("ENGINE", f" IPv6 Routes:    {len(final_v6)}")
            log("ENGINE", f" Blocked:        {len(in_ad)}")
            log("ENGINE", f" Proxied:        {len(proxy_domains)}")
            log("ENGINE", f" Casino Clean:   {cas_total}")
            log("ENGINE", "=============================================")
            log("ENGINE", "Status: SUCCESS")
        except Exception:
            log("ENGINE", f"CRITICAL CRASH: {traceback.format_exc()}", "ERROR")


if __name__ == "__main__":
    if LOCK_FILE.exists():
        try:
            pid = int(LOCK_FILE.read_text().strip())
            if os.path.exists(f"/proc/{pid}"):
                with open(f"/proc/{pid}/cmdline") as f:
                    cmdline = f.read()
                    if "process.py" in cmdline:
                        log("ENGINE", f"Process {pid} already active", "WARNING")
                        sys.exit(0)
        except Exception:
            pass
    LOCK_FILE.write_text(str(os.getpid()))
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
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()
