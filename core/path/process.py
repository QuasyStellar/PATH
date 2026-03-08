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
    print(f"[{t}] [{status:7}] {phase:15} | {msg}", flush=True)


@lru_cache(maxsize=262144)
def validate_domain(line):
    if not line:
        return None
    line = line.partition("#")[0].partition("@")[0].strip().lower()
    for prefix in ["domain:", "keyword:", "full:", "include:"]:
        if line.startswith(prefix):
            line = line[len(prefix):]
            break
    if line.startswith("regexp:") or not line:
        return None
    line = line.strip(".")
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
    line = line.partition("#")[0].strip()
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
            keys = [
                "include-hosts", "exclude-hosts", "include-ips", "exclude-ips",
                "include-adblock-hosts", "exclude-adblock-hosts", "rpz", "rpz2",
                "remove-hosts"
            ]
            for name in keys:
                src = SOURCES_DIR / f"{name}.txt"
                if not src.exists():
                    continue
                tdir = DOWNLOAD_DIR / name
                if tdir.exists():
                    shutil.rmtree(tdir)
                tdir.mkdir(parents=True)
                with open(src) as f:
                    for ln in f:
                        ln = ln.strip()
                        if ln and not ln.startswith("#"):
                            h = hashlib.md5(ln.encode()).hexdigest()[:8]
                            tasks.append(self.fetch(session, ln, tdir / f"{h}.txt"))
            if tasks:
                results = await asyncio.gather(*tasks)
                log("FETCHER", f"Downloaded {sum(results)} lists successfully.")

    def load_single(self, name, base, is_ip=False, f_cas=False):
        raw = []
        path = base / name if base == DOWNLOAD_DIR else base
        files = [path / f"{name}.txt"] if base == MANUAL_DIR else path.glob("*.txt")
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
                d, c = r
                if c and f_cas:
                    self.stats["casino"] += 1
                    continue
                valid.add(d)
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
                res = list(ipaddress.collapse_addresses(
                    [n.supernet(new_prefix=target) if n.prefixlen > target else n
                     for n in res]
                ))
            while len(res) > limit:
                mp = max(n.prefixlen for n in res)
                if mp <= (12 if ver == 4 else 32):
                    break
                res = list(ipaddress.collapse_addresses(
                    [n.supernet() if n.prefixlen == mp else n for n in res]
                ))
        return sorted(res)

    def run(self):
        log("ENGINE", "Processing started")
        in_ips_raw = self.load(["include-ips"], is_ip=True)
        ex_ips_raw = self.load(["exclude-ips"], is_ip=True)

        i_v4 = [ipaddress.ip_network(i, False) for i in in_ips_raw if ":" not in i]
        i_v6 = [ipaddress.ip_network(i, False) for i in in_ips_raw if ":" in i]
        e_v4 = [ipaddress.ip_network(i, False) for i in ex_ips_raw if ":" not in i]
        e_v6 = [ipaddress.ip_network(i, False) for i in ex_ips_raw if ":" in i]

        def sub_nets(inc, exc):
            res = []
            inc = list(ipaddress.collapse_addresses(inc))
            exc = list(ipaddress.collapse_addresses(exc))
            for i_net in inc:
                curr = [i_net]
                for e_net in exc:
                    new = []
                    for c_net in curr:
                        if e_net.overlaps(c_net):
                            try:
                                new.extend(list(c_net.address_exclude(e_net)))
                            except Exception:
                                if not (e_net.network_address <= c_net.network_address
                                        and e_net.broadcast_address >=
                                        c_net.broadcast_address):
                                    new.append(c_net)
                        else:
                            new.append(c_net)
                    curr = new
                res.extend(curr)
            return res

        final_v4 = sub_nets(i_v4, e_v4)
        limit = int(self.env.get("AGGREGATE_COUNT", 500))
        v4 = self.aggregate({str(i) for i in final_v4}, limit, 4)
        with open(RESULT_DIR / "route-ips.txt", "w") as f:
            for n in v4:
                f.write(f"{n}\n")
        self.stats["v4"] = len(v4)
        log("ROUTING", f"Aggregated {len(v4)} IPv4 prefixes")

        if self.env.get("ENABLE_IPV6") == "y":
            final_v6 = sub_nets(i_v6, e_v6)
            v6 = self.aggregate({str(i) for i in final_v6}, limit, 6)
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
                                d, c = v
                                if is_ex:
                                    ex_ad.add(d)
                                else:
                                    in_ad.add(d)

        rpz_h = self.load(["rpz"])
        rpz2_h = self.load(["rpz2"])
        in_ad.update(rpz_h)
        in_ad_opt = set(optimize_trie(in_ad))
        ex_ad_opt = set(optimize_trie(ex_ad))
        log("ADBLOCK", f"Total blocked domains: {len(in_ad_opt)}")

        log("POLICY", "Optimizing proxy rules...")
        f_cas = self.env.get("FILTER_CASINO") == "y"
        i_m = self.load_single("include-hosts", MANUAL_DIR, f_cas=f_cas)
        i_d = self.load_single("include-hosts", DOWNLOAD_DIR, f_cas=f_cas)
        e_m = self.load_single("exclude-hosts", MANUAL_DIR)
        e_d = self.load_single("exclude-hosts", DOWNLOAD_DIR)
        rem = self.load(["remove-hosts"])

        ex_f = (((e_d | ex_ad) - i_m) | e_m) - in_ad - rem
        proxy = ((i_m | i_d) - ex_f) - in_ad - rem

        if self.env.get("ROUTE_ALL") == "y":
            proxy |= {"."}

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
                    lines.extend([f"{d}. CNAME rpz-passthru.",
                                 f"*.{d}. CNAME rpz-passthru."])
            dest = RESULT_DIR / f"{name}.rpz"
            content = "\n".join(lines) + "\n"
            if dest.exists() and dest.read_text() == content:
                return False
            dest.write_text(content)
            return True

        changed = write_rpz("deny", in_ad_opt, ex_ad_opt)
        changed = write_rpz("deny2", rpz2_h, set()) or changed
        changed = write_rpz("proxy", proxy_opt, excl_opt,
                           ra=(self.env.get("ROUTE_ALL") == "y")) or changed
        if changed:
            log("SYNC", "Applying changes to DNS server...")
            for z in ["deny", "deny2", "proxy"]:
                src = RESULT_DIR / f"{z}.rpz"
                if src.exists():
                    shutil.copy2(src, KNOT_DIR / f"{z}.rpz")
            for i in [1, 2]:
                cmd = (f"echo 'cache.clear()' | socat - "
                       f"unix-connect:/run/knot-resolver/control/{i} >/dev/null 2>&1")
                os.system(cmd)
        print("\n" + "=" * 45)
        print("         PATH (Policy-Aware Traffic Handler)")
        print("=" * 45)
        print(f" IPv4 Routes:    {self.stats.get('v4', 0)}")
        print(f" IPv6 Routes:    {self.stats.get('v6', 0)}")
        print(f" Blocked:        {len(in_ad_opt)}")
        print(f" Proxied:        {self.stats['proxy']}")
        print(f" Casino Clean:   {self.stats['casino']}")
        print("=" * 45)
        print(" Status: SUCCESS\n")


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
