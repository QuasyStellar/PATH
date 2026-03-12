#!/usr/bin/env python3

import asyncio
import aiohttp
import hashlib
import re
import time
import ipaddress
import shutil
import idna
import subprocess
import os
from pathlib import Path
from multiprocessing import Pool, cpu_count

import redis

WORKDIR = Path(__file__).parent.absolute()
LISTS_DIR = WORKDIR / "lists"
SOURCES_DIR = LISTS_DIR / "sources"
MANUAL_DIR = LISTS_DIR / "manual"
RESULT_DIR = WORKDIR / "result"
DOWNLOAD_DIR = WORKDIR / "download"
KNOT_DIR = Path("/etc/knot-resolver")
LOCK_FILE = Path("/tmp/path_process.lock")

MAX_CONCURRENT_DOWNLOADS = 10

CASINO_REGEX = re.compile(
    r"([ck]a[szc3][iley1]n[0-9o]|vulkan|vlk|v[uy]l[kc]an|va[vw]ada|x.*bet|most.*bet|leon"
    r".*bet|rio.*bet|mel.*bet|ramen.*bet|marathon|max.*bet|bet.*win|gg-*bet|spin.*bet"
    r"|banzai|1iks|x.*slot|sloto.*zal|bk.*leon|gold.*fishka|play.*fortuna|dragon.*money"
    r"|poker.*dom|1.*win|crypto.*bos|free.*spin|fair.*spin|no.*deposit|igrovye|avtomaty"
    r"|bookmaker|zerkalo|official|slottica|sykaaa|admiral|pinup|pari.*match|betting|"
    r"partypoker|jackpot|bonus|azino|888.*starz|zooma|zenit|eldorado|slots|vodka|"
    r"newretro|platinum|igrat|flagman|arkada|game.*top|vavada|joy.*casino|sol.*casino|"
    r"roxb.*|riobet|fresh.*cas|izzy.*cas|legzo.*cas|volna.*cas|starda.*cas|drip.*cas|"
    r"\.bet$|\.casino$)",
    re.I,
)

LABEL_RE = re.compile(r"^(?!-)[a-z0-9-]{1,63}(?<!-)$", re.I)


def log(phase, msg, status="INFO"):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] [{status:4}] {phase:15} | {msg}", flush=True)


def build_trie_shard(domains):
    root = {}
    for domain in domains:
        parts = domain.split(".")
        curr = root
        for i in range(len(parts) - 1, -1, -1):
            p = parts[i]
            if p in curr:
                if curr[p] is True:
                    break
                if i == 0:
                    curr[p] = True
                    break
                curr = curr[p]
            else:
                if i == 0:
                    curr[p] = True
                else:
                    curr[p] = {}
                    curr = curr[p]

    def extract(node, path, res):
        for k, v in node.items():
            if v is True:
                res.append(".".join([k] + path))
            else:
                extract(v, [k] + path, res)

    res = []
    extract(root, [], res)
    return res


def optimize_trie(domain_list, pool):
    if not domain_list:
        return []
    if len(domain_list) < 1000:
        return build_trie_shard(domain_list)
    shards = {}
    for d in domain_list:
        suffix = d.split(".")[-1]
        if suffix not in shards:
            shards[suffix] = []
        shards[suffix].append(d)
    results = pool.map(build_trie_shard, shards.values())
    final = []
    for r in results:
        final.extend(r)
    return final


def validate_domain(line):
    if not line:
        return None
    line = line.partition("#")[0].partition("@")[0].strip().lower()
    for prefix in ["domain:", "keyword:", "full:", "include:"]:
        if line.startswith(prefix):
            line = line[len(prefix) :]
            break
    if line.startswith("regexp:") or not line:
        return None
    line = line.strip(".")
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


def validate_file(args):
    path, is_ip, f_cas, casino_regex_pattern = args
    import re

    c_re = re.compile(casino_regex_pattern, re.I)
    res, cas_count = set(), 0
    try:
        with open(path, "r", errors="ignore") as f:
            for line in f:
                line = line.partition("#")[0].partition("@")[0].strip().lower()
                if not line:
                    continue
                if is_ip:
                    try:
                        ipaddress.ip_network(line, strict=False)
                        res.add(line)
                    except Exception:
                        pass
                else:
                    for prefix in ["domain:", "keyword:", "full:", "include:"]:
                        if line.startswith(prefix):
                            line = line[len(prefix) :]
                            break
                    if line.startswith("regexp:") or not line:
                        continue
                    line = line.strip(".")
                    if c_re.search(line):
                        if f_cas:
                            cas_count += 1
                            continue
                    if not all(ord(c) < 128 for c in line):
                        try:
                            line = idna.encode(line).decode("ascii")
                        except Exception:
                            continue
                    labels = line.split(".")
                    if len(labels) < 2 or len(line) > 253:
                        continue
                    valid = True
                    for label in labels:
                        if not LABEL_RE.match(label):
                            valid = False
                            break
                    if valid:
                        res.add(line)
    except Exception:
        pass
    return list(res), cas_count


class Processor:
    def __init__(self, env):
        self.env = env
        for d in [RESULT_DIR, DOWNLOAD_DIR]:
            d.mkdir(parents=True, exist_ok=True)
        self.cores = cpu_count()
        self.pool = Pool(self.cores)
        self.stats = {"casino": 0, "v4": 0, "v6": 0, "proxy": 0}
        self.r = None
        if env.get("REDIS_URL"):
            try:
                params = {"decode_responses": False}
                pw = env.get("REDIS_PASSWORD")
                if pw:
                    params["password"] = pw
                self.r = redis.from_url(env["REDIS_URL"], **params)
                self.r.ping()
            except Exception:
                self.r = None

    def sync_to_redis(self, h):
        if not self.r:
            return
        log("REDIS", "Pushing results to cluster storage...")
        pipe = self.r.pipeline()
        for z in ["deny", "deny2", "proxy"]:
            f = RESULT_DIR / f"{z}.rpz"
            if f.exists():
                import zlib

                data = zlib.compress(f.read_bytes())
                pipe.set(f"path:data:{z}", data)
        pipe.set("path:data:hash", h.encode())
        pipe.publish("path:sync", "reload")
        pipe.execute()

    def sync_from_redis(self):
        if not self.r:
            return False
        log("REDIS", "Pulling results from cluster storage...")
        try:
            h = self.r.get("path:data:hash")
            if not h:
                return False
            for z in ["deny", "deny2", "proxy"]:
                data = self.r.get(f"path:data:{z}")
                if data:
                    import zlib

                    dest = RESULT_DIR / f"{z}.rpz"
                    tmp = dest.with_suffix(".tmp")
                    tmp.write_bytes(zlib.decompress(data))
                    tmp.replace(dest)
            (RESULT_DIR / ".hash").write_text(h.decode())
            self.sync_to_knot()
            return True
        except Exception as e:
            log("REDIS", f"Sync failed: {e}", "WARNING")
            return False

    def __del__(self):
        if hasattr(self, "pool"):
            self.pool.close()
            self.pool.join()

    def get_state_hash(self):
        h = hashlib.md5()
        for p in [SOURCES_DIR, MANUAL_DIR]:
            for f in sorted(p.glob("*.txt")):
                h.update(f.read_bytes())
        for f in sorted(DOWNLOAD_DIR.rglob("*.txt")):
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
        ]:
            v = self.env.get(k, "")
            h.update(f"{k}={v}".encode())
        return h.hexdigest()

    async def fetch(self, session, url, path, semaphore):
        async with semaphore:
            try:
                async with session.get(url, timeout=30) as r:
                    if r.status == 200:
                        data = await r.read()
                        if url.endswith(".gz"):
                            import gzip

                            data = gzip.decompress(data)
                        if len(data) < 50:
                            return False
                        low_data = data[:512].lower()
                        if (
                            b"<html" in low_data
                            or b"<!doctype" in low_data
                            or b"<title" in low_data
                        ):
                            return False
                        if path.exists() and path.read_bytes() == data:
                            return True
                        with open(path, "wb") as f:
                            f.write(data)
                        return True
            except Exception:
                pass
        return False

    async def update_sources(self):
        log("FETCHER", "Starting sources update...")
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        async with aiohttp.ClientSession() as session:
            tasks = []
            keys = [
                "include-hosts",
                "exclude-hosts",
                "include-ips",
                "exclude-ips",
                "include-adblock-hosts",
                "exclude-adblock-hosts",
                "rpz",
                "rpz2",
                "remove-hosts",
            ]
            if self.env.get("BLOCK_ADS") != "y":
                keys = [
                    k
                    for k in keys
                    if k
                    not in [
                        "include-adblock-hosts",
                        "exclude-adblock-hosts",
                        "rpz",
                        "rpz2",
                    ]
                ]
            for name in keys:
                src = SOURCES_DIR / f"{name}.txt"
                if not src.exists():
                    continue
                tdir = DOWNLOAD_DIR / name
                if not tdir.exists():
                    tdir.mkdir(parents=True)
                existing_files = {f.name for f in tdir.glob("*.txt")}
                new_files = set()
                with open(src) as f:
                    for ln in f:
                        ln = ln.strip()
                        if ln and not ln.startswith("#"):
                            h = hashlib.md5(ln.encode()).hexdigest()
                            fname = f"{h}.txt"
                            new_files.add(fname)
                            tasks.append(
                                self.fetch(session, ln, tdir / fname, semaphore)
                            )
                for old_f in existing_files - new_files:
                    try:
                        (tdir / old_f).unlink()
                    except Exception:
                        pass
            if tasks:
                results = await asyncio.gather(*tasks)
                log("FETCHER", f"Fetched {len(tasks)} URLs, {sum(results)} successful.")

    def load_single(self, name, base, is_ip=False, f_cas=False):
        path = base / name if base == DOWNLOAD_DIR else base
        files = [path / f"{name}.txt"] if base == MANUAL_DIR else path.glob("*.txt")
        tasks = [
            (str(f), is_ip, f_cas, CASINO_REGEX.pattern) for f in files if f.exists()
        ]
        if not tasks:
            return set()
        results = self.pool.map(validate_file, tasks)
        valid = set()
        for res_list, cas in results:
            valid.update(res_list)
            self.stats["casino"] += cas
        return valid

    def load(self, names, is_ip=False, f_cas=False):
        valid = set()
        for n in names:
            valid.update(self.load_single(n, DOWNLOAD_DIR, is_ip, f_cas))
            valid.update(self.load_single(n, MANUAL_DIR, is_ip, f_cas))
        return valid

    def aggregate(self, ips, limit, ver=4):
        if not ips:
            return []
        nets = [ipaddress.ip_network(i, strict=False) for i in ips]
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
                src_h = hashlib.md5(src.read_bytes()).hexdigest()
                dst_h = (
                    hashlib.md5(dst.read_bytes()).hexdigest() if dst.exists() else ""
                )
                if src_h != dst_h:
                    tmp = dst.with_suffix(".tmp")
                    shutil.copy2(src, tmp)
                    tmp.replace(dst)
                    changed = True
        if changed:
            for i in [1, 2]:
                cmd = f"echo 'cache.clear()' | socat - unix-connect:/run/knot-resolver/control/{i}"
                try:
                    subprocess.run(cmd, shell=True, capture_output=True, timeout=5)
                except Exception:
                    pass

    async def run(self):
        role = self.env.get("NODE_ROLE", "solo").lower()
        if role == "worker":
            if self.sync_from_redis():
                log("ENGINE", "Worker sync completed successfully")
                return
            log(
                "ENGINE",
                "Worker sync failed, falling back to local generation",
                "WARNING",
            )
        await self.update_sources()
        h_file = RESULT_DIR / ".hash"
        new_h = self.get_state_hash()
        required_files = ["proxy.rpz", "deny.rpz"]
        all_files_valid = all(
            (RESULT_DIR / f).exists() and (RESULT_DIR / f).stat().st_size > 0
            for f in required_files
        )
        if h_file.exists() and h_file.read_text() == new_h and all_files_valid:
            log("ENGINE", "No changes detected, skipping generation")
            self.sync_to_knot()
            if self.r:
                self.sync_to_redis(new_h)
            return
        log("ENGINE", "Processing started")
        in_ips_raw, ex_ips_raw = (
            self.load(["include-ips"], is_ip=True),
            self.load(["exclude-ips"], is_ip=True),
        )
        i_v4 = [ipaddress.ip_network(i, False) for i in in_ips_raw if ":" not in i]
        i_v6 = [ipaddress.ip_network(i, False) for i in in_ips_raw if ":" in i]
        e_v4 = [ipaddress.ip_network(i, False) for i in ex_ips_raw if ":" not in i]
        e_v6 = [ipaddress.ip_network(i, False) for i in ex_ips_raw if ":" in i]

        def sub_nets(inc, exc):
            res = []
            inc, exc = (
                list(ipaddress.collapse_addresses(inc)),
                list(ipaddress.collapse_addresses(exc)),
            )
            for i_net in inc:
                curr = [i_net]
                for e_net in exc:
                    new = []
                    for c_net in curr:
                        if e_net.overlaps(c_net):
                            try:
                                new.extend(list(c_net.address_exclude(e_net)))
                            except Exception:
                                if not (
                                    e_net.network_address <= c_net.network_address
                                    and e_net.broadcast_address
                                    >= c_net.broadcast_address
                                ):
                                    new.append(c_net)
                        else:
                            new.append(c_net)
                    curr = new
                res.extend(curr)
            return res

        v4 = self.aggregate(
            {str(i) for i in sub_nets(i_v4, e_v4)},
            int(self.env.get("AGGREGATE_COUNT", 500)),
            4,
        )
        self.stats["v4"] = len(v4)
        dest_v4 = RESULT_DIR / "route-ips.txt"
        tmp_v4 = dest_v4.with_suffix(".tmp")
        with open(tmp_v4, "w") as f:
            for n in v4:
                f.write(f"{n}\n")
        tmp_v4.replace(dest_v4)
        if self.env.get("ENABLE_IPV6") == "y":
            v6 = self.aggregate(
                {str(i) for i in sub_nets(i_v6, e_v6)},
                int(self.env.get("AGGREGATE_COUNT", 500)),
                6,
            )
            self.stats["v6"] = len(v6)
            dest_v6 = RESULT_DIR / "route-ips-v6.txt"
            tmp_v6 = dest_v6.with_suffix(".tmp")
            with open(tmp_v6, "w") as f:
                for n in v6:
                    f.write(f"{n}\n")
            tmp_v6.replace(dest_v6)
        log("ADBLOCK", "Compiling host lists...")
        in_ad, ex_ad = set(), set()
        if self.env.get("BLOCK_ADS") == "y":
            for name in ["include-adblock-hosts", "exclude-adblock-hosts"]:
                for p in [DOWNLOAD_DIR / name, MANUAL_DIR]:
                    files = [p / f"{name}.txt"] if p == MANUAL_DIR else p.glob("*.txt")
                    for f in files:
                        if not f.exists():
                            continue
                        with open(f, "r", errors="ignore") as fd:
                            for ln in fd:
                                l_line = ln.strip().lower()
                                if not l_line or l_line.startswith("!"):
                                    continue
                                is_ex = l_line.startswith("@@")
                                if l_line.startswith("@@||"):
                                    l_line = l_line[4:]
                                elif l_line.startswith("@@"):
                                    l_line = l_line[2:]
                                elif l_line.startswith("||"):
                                    l_line = l_line[2:]
                                l_line = l_line.partition("^")[0].partition("$")[0]
                                d = validate_domain(l_line)
                                if d:
                                    if is_ex:
                                        ex_ad.add(d)
                                    else:
                                        in_ad.add(d)
            in_ad.update(self.load(["rpz"]))
        in_ad_opt, ex_ad_opt = (
            set(optimize_trie(in_ad, self.pool)),
            set(optimize_trie(ex_ad, self.pool)),
        )
        log("POLICY", "Optimizing proxy rules...")
        f_cas = self.env.get("FILTER_CASINO") == "y"
        i_m, i_d = (
            self.load_single("include-hosts", MANUAL_DIR, f_cas=f_cas),
            self.load_single("include-hosts", DOWNLOAD_DIR, f_cas=f_cas),
        )
        e_m, e_d, rem = (
            self.load_single("exclude-hosts", MANUAL_DIR),
            self.load_single("exclude-hosts", DOWNLOAD_DIR),
            self.load(["remove-hosts"]),
        )
        ex_f = (((e_d | ex_ad_opt) - i_m) | e_m) - in_ad_opt - rem
        proxy = ((i_m | i_d) - ex_f) - in_ad_opt - rem
        if self.env.get("ROUTE_ALL") == "y":
            proxy |= {"."}
        proxy_opt, excl_opt = (
            sorted(optimize_trie(proxy, self.pool)),
            sorted(optimize_trie(ex_f, self.pool)),
        )
        self.stats["proxy"] = len(proxy_opt)

        def write_rpz(name, inc, ex, ra=False):
            lines = ["$TTL 10800", "@ SOA . . (1 1 1 1 10800)"]
            if ra and name == "proxy":
                lines.append("* CNAME .")
            for d in sorted(inc):
                if d != ".":
                    lines.extend([f"{d}. CNAME .", f"*.{d}. CNAME ."])
            for d in sorted(ex):
                if d != ".":
                    lines.extend(
                        [f"{d}. CNAME rpz-passthru.", f"*.{d}. CNAME rpz-passthru."]
                    )
            content = "\n".join(lines) + "\n"
            dest = RESULT_DIR / f"{name}.rpz"
            if dest.exists() and dest.read_text() == content:
                return False
            tmp = dest.with_suffix(".tmp")
            with open(tmp, "w") as f:
                f.write(content)
            tmp.replace(dest)
            return True

        write_rpz("deny", in_ad_opt, ex_ad_opt)
        if self.env.get("BLOCK_ADS") == "y":
            write_rpz("deny2", self.load(["rpz2"]), set())
        write_rpz("proxy", proxy_opt, excl_opt, ra=(self.env.get("ROUTE_ALL") == "y"))

        log("ENGINE", "\n" + "=" * 45)
        log("ENGINE", "         PATH (Policy-Aware Traffic Handler)")
        log("ENGINE", "=" * 45)
        log("ENGINE", f" IPv4 Routes:    {self.stats.get('v4', 0)}")
        log("ENGINE", f" IPv6 Routes:    {self.stats.get('v6', 0)}")
        log("ENGINE", f" Blocked:        {len(in_ad_opt)}")
        log("ENGINE", f" Proxied:        {self.stats['proxy']}")
        log("ENGINE", f" Casino Clean:   {self.stats['casino']}")
        log("ENGINE", "=" * 45)
        log("ENGINE", " Status: SUCCESS")

        self.sync_to_knot()
        if self.r:
            self.sync_to_redis(new_h)
        h_file.write_text(new_h)


def load_env(path):
    env = {}
    if not path.exists():
        return env
    with open(path) as f:
        for ln in f:
            ln = ln.strip()
            if ln and not ln.startswith("#"):
                k, _, v = ln.partition("=")
                env[k.strip()] = v.strip()
    return env


if __name__ == "__main__":
    locked = False
    for i in range(2):
        try:
            fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode())
            os.close(fd)
            locked = True
            break
        except FileExistsError:
            try:
                with open(LOCK_FILE, "r") as f:
                    pid = int(f.read().strip())
                os.kill(pid, 0)
                log("ENGINE", f"Process {pid} is already running, exiting.")
                exit(0)
            except (ProcessLookupError, ValueError, FileNotFoundError):
                try:
                    os.unlink(str(LOCK_FILE))
                except Exception:
                    pass
    if not locked:
        log("ENGINE", "Failed to acquire lock, exiting.")
        exit(0)
    try:
        e = load_env(WORKDIR / ".env")
        asyncio.run(Processor(e).run())
    finally:
        try:
            os.unlink(str(LOCK_FILE))
        except Exception:
            pass
