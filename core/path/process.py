#!/usr/bin/env python3

import asyncio
import aiohttp
import hashlib
import os
import re
import time
import ipaddress
import shutil
import idna
from pathlib import Path
from multiprocessing import Pool, cpu_count
from functools import lru_cache

WORKDIR = Path(__file__).parent.absolute()
LISTS_DIR = WORKDIR / "lists"
SOURCES_DIR = LISTS_DIR / "sources"
MANUAL_DIR = LISTS_DIR / "manual"
RESULT_DIR = WORKDIR / "result"
DOWNLOAD_DIR = WORKDIR / "download"
KNOT_DIR = Path("/etc/knot-resolver")

PROXY_URL = "https://api.codetabs.com/v1/proxy?quest="

CASINO_REGEX = re.compile(
    r"([ck]a[szc3][iley1]n[0-9o]|vulkan|vlk|v[uy]l[kc]an|va[vw]ada|x.*bet|most.*bet|leon.*bet|rio.*bet|"
    r"mel.*bet|ramen.*bet|marathon|max.*bet|bet.*win|gg-*bet|spin.*bet|banzai|1iks|x.*slot|sloto.*zal|"
    r"bk.*leon|gold.*fishka|play.*fortuna|dragon.*money|poker.*dom|1.*win|crypto.*bos|free.*spin|"
    r"fair.*spin|no.*deposit|igrovye|avtomaty|bookmaker|zerkalo|official|slottica|sykaaa|admiral|pinup|"
    r"pari.*match|betting|partypoker|jackpot|bonus|azino|888.*starz|zooma|zenit|eldorado|slots|vodka|"
    r"newretro|platinum|igrat|flagman|arkada|game.*top|vavada|joy.*casino|sol.*casino|roxb.*|"
    r"riobet|fresh.*cas|izzy.*cas|legzo.*cas|volna.*cas|starda.*cas|drip.*cas|\.bet$|\.casino$)",
    re.I,
)

LABEL_RE = re.compile(r"^(?!-)[a-z0-9-]{1,63}(?<!-)$", re.I)


def log(phase, msg, status="INFO"):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] [{status:7}] {phase:15} | {msg}", flush=True)


@lru_cache(maxsize=262144)
def validate_domain(line):
    if not line:
        return None
    line = line.lower().strip().strip(".")
    is_casino = bool(CASINO_REGEX.search(line))
    if not all(ord(c) < 128 for c in line):
        try:
            line = idna.encode(line).decode("ascii")
        except Exception:
            return None
    labels = line.split(".")
    if len(labels) < 2:
        return None
    for label in labels:
        if not LABEL_RE.match(label):
            return None
    if len(line) > 253:
        return None
    return (line, is_casino)


def validate_ip(line):
    line = line.strip()
    if not line:
        return None
    try:
        ipaddress.ip_network(line, strict=False)
        return line
    except Exception:
        return None


def optimize_trie(domain_list):
    root = {}
    for domain in domain_list:
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
    res = []
    stack = [(root, [])]
    while stack:
        node, path = stack.pop()
        for k, v in node.items():
            if v is True:
                res.append(".".join([k] + path))
            else:
                stack.append((v, [k] + path))
    return res


class Processor:
    def __init__(self, env):
        self.env = env
        for d in [RESULT_DIR, DOWNLOAD_DIR]:
            d.mkdir(parents=True, exist_ok=True)
        self.cores = cpu_count()
        self.stats = {"casino": 0}

    async def fetch(self, session, url, path):
        try:
            async with session.get(url, timeout=30) as r:
                if r.status == 200:
                    data = await r.read()
                    if url.endswith(".gz"):
                        import gzip

                        data = gzip.decompress(data)
                    with open(path, "wb") as f:
                        f.write(data)
                    return True
        except Exception:
            pass
        if not url.startswith(PROXY_URL):
            try:
                async with session.get(PROXY_URL + url, timeout=30) as resp:
                    if resp.status == 200:
                        with open(path, "wb") as f:
                            f.write(await resp.read())
                        return True
            except Exception:
                pass
        return False

    async def update_sources(self):
        log("FETCHER", "Starting sources update...")
        async with aiohttp.ClientSession() as session:
            tasks = []
            for source_name in [
                "include-hosts",
                "exclude-hosts",
                "include-ips",
                "exclude-ips",
                "include-adblock-hosts",
                "exclude-adblock-hosts",
                "rpz",
                "rpz2",
                "remove-hosts",
            ]:
                src = SOURCES_DIR / f"{source_name}.txt"
                if not src.exists():
                    continue
                tdir = DOWNLOAD_DIR / source_name
                if tdir.exists():
                    shutil.rmtree(tdir)
                tdir.mkdir(parents=True)
                with open(src) as f:
                    for ln in f:
                        ln = ln.strip()
                        if ln and not ln.startswith("#"):
                            tasks.append(
                                self.fetch(
                                    session,
                                    ln,
                                    tdir
                                    / (hashlib.md5(ln.encode()).hexdigest()[:8] + ".txt"),
                                )
                            )
            if tasks:
                results = await asyncio.gather(*tasks)
                log("FETCHER", f"Downloaded {sum(results)} lists successfully.")

    def load(self, names, is_ip=False, f_cas=False):
        raw = []
        for n in names:
            for p in [DOWNLOAD_DIR / n, MANUAL_DIR]:
                files = [p / f"{n}.txt"] if p == MANUAL_DIR else p.glob("*.txt")
                for f in files:
                    if f.exists():
                        with open(f, "r", errors="ignore") as fd:
                            raw.extend(fd.readlines())
        if not raw:
            return set()
        func = validate_ip if is_ip else validate_domain
        with Pool(self.cores) as pool:
            results = pool.map(func, raw)
        valid = set()
        for r in results:
            if not r:
                continue
            if is_ip:
                valid.add(r)
            else:
                domain, is_casino = r
                if is_casino and f_cas:
                    self.stats["casino"] += 1
                    continue
                valid.add(domain)
        return valid

    def aggregate(self, ips, limit, ver=4):
        if not ips:
            return []
        nets = []
        for i in ips:
            try:
                nets.append(ipaddress.ip_network(i, strict=False))
            except Exception:
                continue
        res = list(ipaddress.collapse_addresses(nets))
        if limit > 0 and len(res) > limit:
            target = 24 if ver == 4 else 64
            if len(res) > limit * 3:
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

    def run(self):
        log("ENGINE", "Processing started")
        in_ips = self.load(["include-ips"], is_ip=True)
        ex_ips = self.load(["exclude-ips"], is_ip=True)
        final_ips = in_ips - ex_ips
        limit = int(self.env.get("AGGREGATE_COUNT", 500))
        v4 = self.aggregate({i for i in final_ips if ":" not in i}, limit, 4)
        with open(RESULT_DIR / "route-ips.txt", "w") as f:
            for n in v4:
                f.write(f"{n}\n")
        self.stats["v4"] = len(v4)
        log("ROUTING", f"Aggregated {len(v4)} IPv4 prefixes")
        if self.env.get("ENABLE_IPV6") == "y":
            v6 = self.aggregate({i for i in final_ips if ":" in i}, limit, 6)
            with open(RESULT_DIR / "route-ips-v6.txt", "w") as f:
                for n in v6:
                    f.write(f"{n}\n")
            self.stats["v6"] = len(v6)
            log("ROUTING", f"Aggregated {len(v6)} IPv6 prefixes")
        log("ADBLOCK", "Compiling host lists...")
        in_ad, ex_ad = set(), set()
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
                            v = validate_domain(l_line)
                            if v:
                                domain, is_casino = v
                                if is_ex:
                                    ex_ad.add(domain)
                                else:
                                    in_ad.add(domain)
        rpz_h = self.load(["rpz"])
        rpz2_h = self.load(["rpz2"])
        in_ad.update(rpz_h)
        in_ad_opt = set(optimize_trie(in_ad))
        ex_ad_opt = set(optimize_trie(ex_ad))
        log("ADBLOCK", f"Total blocked domains: {len(in_ad_opt)}")
        log("POLICY", "Optimizing proxy rules...")
        f_cas = self.env.get("FILTER_CASINO") == "y"
        inc = self.load(["include-hosts"], f_cas=f_cas)
        exc = self.load(["exclude-hosts"])
        rem = self.load(["remove-hosts"])
        in_f = (inc - rem) - in_ad
        ex_f = (exc - rem) | ex_ad
        proxy = ({"."} if self.env.get("ROUTE_ALL") == "y" else set()) | in_f
        if self.env.get("ROUTE_ALL") != "y":
            proxy -= ex_f
        proxy_opt = sorted(optimize_trie(proxy))
        excl_opt = sorted(optimize_trie(ex_f))
        self.stats["proxy"] = len(proxy_opt)
        with open(RESULT_DIR / "include-hosts.txt", "w") as f:
            for d in proxy_opt:
                f.write(f"{d}\n")
        with open(RESULT_DIR / "exclude-hosts.txt", "w") as f:
            for d in excl_opt:
                f.write(f"{d}\n")
        log("POLICY", f"Total proxied domains: {len(proxy_opt)}")

        def write_rpz(name, inc, ex, ra=False):
            lines = ["$TTL 10800", "@ SOA . . (1 1 1 1 10800)"]
            if ra and name == "proxy":
                lines.append("* CNAME .")
            for d in sorted(inc):
                if d != ".":
                    lines.extend([f"{d}. CNAME .", f"*.{d}. CNAME ."])
            for d in sorted(ex):
                if d != ".":
                    lines.extend([f"{d}. CNAME rpz-passthru.", f"*.{d}. CNAME rpz-passthru."])
            dest = RESULT_DIR / f"{name}.rpz"
            content = "\n".join(lines) + "\n"
            if dest.exists() and dest.read_text() == content:
                return False
            dest.write_text(content)
            return True

        changed = write_rpz("deny", in_ad_opt, ex_ad_opt)
        changed = write_rpz("deny2", rpz2_h, set()) or changed
        changed = write_rpz("proxy", proxy_opt, excl_opt, ra=(self.env.get("ROUTE_ALL") == "y")) or changed
        if changed:
            log("SYNC", "Applying changes to DNS server...")
            for z in ["deny", "deny2", "proxy"]:
                src = RESULT_DIR / f"{z}.rpz"
                if src.exists():
                    shutil.copy2(src, KNOT_DIR / f"{z}.rpz")
            for i in [1, 2]:
                os.system(f"echo 'cache.clear()' | socat - unix-connect:/run/knot-resolver/control/{i} >/dev/null 2>&1")
        print("\n" + "=" * 45 + "\n         PATH (Policy-Aware Traffic Handler)\n" + "=" * 45)
        print(f" IPv4 Routes:    {self.stats.get('v4', 0)}\n IPv6 Routes:    {self.stats.get('v6', 0)}\n Blocked:        {len(in_ad_opt)}\n Proxied:        {self.stats['proxy']}\n Casino Clean:   {self.stats['casino']}\n" + "=" * 45 + "\n Status: SUCCESS\n")


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
    e = load_env(WORKDIR / ".env")
    p = Processor(e)
    asyncio.run(p.update_sources())
    p.run()
