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
    r"[ck]a+[szc3]+[iley1]+n+[0-9o]|[vw][uy]+[l1]+[kc]a+n|[vw]a+[vw]+a+d+a|x-*bet|most-*bet|leon-*bet|rio-*bet|mel-*bet|ramen-*bet|marathon-*bet|max-*bet|bet-*win|gg-*bet|spin-*bet|banzai-*bet|1iks-*bet|x-*slot|sloto.*zal|max-*slot|bk-*leon|gold-*fishka|play-*fortuna|dragon.*money|poker.*dom|1-*win|crypto-*bos|free-*spin|fair-*spin|no-*deposit|igrovye|avtomaty|bookmaker|zerkalo|slottica|sykaaa|admiral-*x|x-*admiral|pinup-*bet|pari-*match|betting|partypoker|jackpot|bonus|azino[0-9-]|888-*starz|zooma[0-9-]|zenit-*bet|eldorado|slots|vodka|newretro|platinum|igrat|flagman|arkada",
    re.I,
)

LABEL_RE = re.compile(r"^[a-z0-9_]([a-z0-9-_]{0,61}[a-z0-9_])?$", re.I)
PREFIX_RE = re.compile(r"^([0-9]*www[0-9]*|hd[0-9]*|[A-Za-z]|[0-9]+)\.", re.I)


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
    if not line:
        return None
    is_wildcard = line.startswith("*.")
    domain_part = line[2:] if is_wildcard else line
    domain_part = _normalize_domain_candidate(domain_part)
    if not domain_part or "." not in domain_part or len(domain_part) > 253:
        return None
    labels = domain_part.split(".")
    for label in labels:
        if not label or not LABEL_RE.match(label):
            return None
    return ("*." + domain_part) if is_wildcard else domain_part


@lru_cache(maxsize=262144)
def parse_adblock_line(line, force_exception=False):
    if not line:
        return None
    line = line.strip()
    if (
        not line
        or line.startswith("!")
        or line.startswith("[")
        or "##" in line
        or "#@#" in line
    ):
        return None
    is_ex, domain = force_exception, None
    if line.startswith("@@||"):
        is_ex, domain = True, line[4:].split("^")[0]
    elif line.startswith("||"):
        is_ex, domain = False, line[2:].split("^")[0]
    elif line.startswith("@@"):
        is_ex, domain = True, line[2:].split("^")[0]
    else:
        domain = line
    if not domain or ("*" in domain and not domain.startswith("*.")):
        return None
    v = validate_domain(domain)
    return (v, is_ex) if v else None


def validate_file(
    path, is_ip, f_cas, is_exclude_file=False, is_rpz=False, is_domain=False
):
    res, cas_set, adblock_rules, raw_rules = set(), set(), set(), set()
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return (res if is_ip else adblock_rules), cas_set, raw_rules
    try:
        with open(path, "rb") as f:
            for line_bytes in f:
                try:
                    line = line_bytes.decode("utf-8", errors="replace").strip()
                except Exception:
                    continue
                if not line or line.startswith("#") or line.startswith(";"):
                    continue
                if is_rpz and not is_ip:
                    if line.startswith("$") or line.startswith("@"):
                        continue
                    raw_rules.add(line)
                    continue
                if is_domain and not is_ip:
                    line = line.lstrip("!\"#$%&'()+,-/:;<=>?@[\\]^_`{|}~").rstrip(
                        "!\"#$%&'()*+,-/:;<=>?@[\\]^_`{|}~"
                    )
                    if not line:
                        continue
                    v = validate_domain(line)
                    if v:
                        if f_cas and CASINO_RE.search(v):
                            cas_set.add(v)
                        else:
                            adblock_rules.add((v, is_exclude_file))
                    continue
                if line[0] in "!#[]":
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
                    parsed = parse_adblock_line(line, force_exception=is_exclude_file)
                    if parsed:
                        v, is_ex = parsed
                        if f_cas and CASINO_RE.search(v):
                            cas_set.add(v)
                        else:
                            adblock_rules.add((v, is_ex))
    except Exception:
        pass
    return (res if is_ip else adblock_rules), cas_set, raw_rules


def optimize_trie(domains):
    if not domains:
        return []
    sorted_domains = sorted(domains, key=lambda d: (len(d), d.count(".")))
    trie, res = {}, []
    for d in sorted_domains:
        parts = d.split(".")[::-1]
        curr, is_redundant = trie, False
        for p in parts:
            if "__root__" in curr:
                is_redundant = True
                break
            if p not in curr:
                curr[p] = {}
            curr = curr[p]
        if not is_redundant:
            curr["__root__"] = True
            res.append(d)
    return res


def sub_nets_optimized(inc_nets, exc_ips):
    if not inc_nets:
        return []
    if not exc_ips:
        return sorted(inc_nets)
    ranges = sorted(
        [(int(net.network_address), int(net.broadcast_address)) for net in inc_nets]
    )
    exc_ranges = sorted(
        [(int(net.network_address), int(net.broadcast_address)) for net in exc_ips]
    )
    result_ranges, e_idx = [], 0
    for r_start, r_end in ranges:
        curr_start = r_start
        while e_idx < len(exc_ranges) and exc_ranges[e_idx][1] < curr_start:
            e_idx += 1
        for i in range(e_idx, len(exc_ranges)):
            e_start, e_end = exc_ranges[i]
            if e_start > r_end:
                break
            if e_end < curr_start:
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
            import redis.asyncio as redis

            try:
                self.r = redis.from_url(
                    env["REDIS_URL"],
                    password=env.get("REDIS_PASSWORD"),
                    decode_responses=False,
                )
            except Exception:
                pass

    async def r_get(self, key):
        return await self.r.get(key) if self.r else None

    async def r_set(self, *args, **kwargs):
        return await self.r.set(*args, **kwargs) if self.r else False

    def get_state_hash(self):
        h = hashlib.md5(usedforsecurity=False)
        for p in [SOURCE_DIR, MANUAL_DIR, DOWNLOAD_DIR]:
            if not p.exists():
                continue
            for f in sorted(p.rglob("*.txt")):
                if f.is_relative_to(TEMP_DIR) or f.stat().st_size == 0:
                    continue
                h.update(f"{f.relative_to(WORKDIR)}".encode())
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
            "IP",
            "FAKE_IP",
        ]:
            h.update(f"{k}={self.env.get(k, '')}".encode())
        return h.hexdigest()

    async def update_sources(self):
        url_map = {}
        active_stems = set()
        stems_with_urls = set()
        for f in SOURCE_DIR.glob("*.txt"):
            stem = f.stem
            active_stems.add(stem)
            with open(f) as f_in:
                for line in f_in:
                    u = line.strip()
                    if u.startswith("http"):
                        stems_with_urls.add(stem)
                        if u not in url_map:
                            url_map[u] = []
                        url_map[u].append(
                            TEMP_DIR
                            / "downloads"
                            / stem
                            / f"{hashlib.md5(u.encode(), usedforsecurity=False).hexdigest()[:8]}.txt"
                        )

        for stem_to_clean in active_stems - stems_with_urls:
            d_path = DOWNLOAD_DIR / stem_to_clean
            if d_path.exists() and d_path.is_dir():
                log("FETCHER", f"Cleaning up disabled source: {stem_to_clean}")
                await asyncio.to_thread(shutil.rmtree, d_path)

        if not url_map:
            for existing_dir in DOWNLOAD_DIR.iterdir():
                if (
                    existing_dir.is_dir()
                    and existing_dir.name not in active_stems
                    and existing_dir != TEMP_DIR
                ):
                    log("FETCHER", f"Removing deleted source data: {existing_dir.name}")
                    await asyncio.to_thread(shutil.rmtree, existing_dir)
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
                    for extra_path in paths[1:]:
                        await asyncio.to_thread(
                            extra_path.parent.mkdir, parents=True, exist_ok=True
                        )
                        await asyncio.to_thread(shutil.copy, paths[0], extra_path)
        if temp_download_dir.exists():
            for new_dir in temp_download_dir.iterdir():
                if new_dir.is_dir() and list(new_dir.glob("*.txt")):
                    dest_dir = DOWNLOAD_DIR / new_dir.name
                    if dest_dir.exists():
                        await asyncio.to_thread(shutil.rmtree, dest_dir)
                    await asyncio.to_thread(shutil.move, str(new_dir), str(dest_dir))

            for existing_dir in DOWNLOAD_DIR.iterdir():
                if (
                    existing_dir.is_dir()
                    and existing_dir.name not in active_stems
                    and existing_dir != temp_download_dir.parent
                ):
                    log("FETCHER", f"Removing deleted source data: {existing_dir.name}")
                    await asyncio.to_thread(shutil.rmtree, existing_dir)
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
                            return False
                        if (
                            path.exists()
                            and hashlib.md5(data, usedforsecurity=False).hexdigest()
                            == hashlib.md5(
                                path.read_bytes(), usedforsecurity=False
                            ).hexdigest()
                        ):
                            return True
                        await asyncio.to_thread(path.write_bytes, data)
                        return True
            except Exception:
                pass
        return False

    def load(self, names, is_ip=False, f_cas=False):
        res, all_cas, all_raw = set(), set(), set()
        for name in names:
            is_ex_f = name.startswith("exclude") or name.startswith("remove")
            is_rpz_f = "rpz" in name
            is_ad_f = "adblock" in name
            is_dom_f = ("hosts" in name or "domain" in name) and not is_ad_f
            files = []
            d_path = DOWNLOAD_DIR / name
            if d_path.exists() and d_path.is_dir():
                files.extend(d_path.glob("*.txt"))
            f_dir = DOWNLOAD_DIR / f"{name}.txt"
            if f_dir.exists():
                files.append(f_dir)
            if name == "include-hosts":
                files.extend(DOWNLOAD_DIR.glob("*domain.txt"))
            m_path = MANUAL_DIR / f"{name}.txt"
            if m_path.exists():
                files.append(m_path)
            for f in sorted(list(set(files))):
                out, cas_s, raw_s = validate_file(
                    f,
                    is_ip,
                    f_cas,
                    is_exclude_file=is_ex_f,
                    is_rpz=is_rpz_f,
                    is_domain=is_dom_f,
                )
                res.update(out)
                all_cas.update(cas_s)
                all_raw.update(raw_s)
        return res, all_cas, all_raw

    def aggregate(self, nets, limit, ver=4):
        if not nets:
            return []
        if limit <= 0:
            return nets
        res = list(ipaddress.collapse_addresses(nets))
        if len(res) <= limit:
            return res
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
            if src.exists() and (
                not dst.exists()
                or not await asyncio.to_thread(filecmp.cmp, src, dst, shallow=False)
            ):
                tmp_dst = dst.with_suffix(".tmp")
                await asyncio.to_thread(shutil.copy2, src, tmp_dst)
                await asyncio.to_thread(os.chmod, tmp_dst, 0o644)
                await asyncio.to_thread(tmp_dst.rename, dst)
                changed = True
        if changed:
            ctrl_dir = "/run/knot-resolver/control"
            if os.path.exists(ctrl_dir):
                for s_name in os.listdir(ctrl_dir):
                    try:
                        await asyncio.to_thread(
                            subprocess.run,
                            [
                                "socat",
                                "-",
                                f"unix-connect:{os.path.join(ctrl_dir, s_name)}",
                            ],
                            input=b"cache.clear()\n",
                            capture_output=True,
                            timeout=5,
                        )
                    except Exception:
                        pass

    async def sync_to_redis(self, h, my_id):
        if not self.r:
            return
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
                    pipe.set(
                        f"path:data:{f}",
                        zlib.compress(await asyncio.to_thread(p.read_bytes)),
                    )

            for p_dir in [SOURCE_DIR, MANUAL_DIR]:
                if p_dir.exists():
                    for f_path in p_dir.glob("*.txt"):
                        rel_p = f_path.relative_to(WORKDIR)
                        pipe.set(
                            f"path:list:{rel_p}",
                            await asyncio.to_thread(f_path.read_bytes),
                        )

            pipe.set("path:hash", h)
            pipe.set("path:last_heartbeat", int(time.time()))
            pipe.set("path:master_lock", my_id, ex=3600)
            pipe.publish("path:sync", "reload")
            await pipe.execute()
        except Exception:
            pass

    async def sync_from_redis(self):
        if not self.r:
            return False
        try:
            remote_h = await self.r.get("path:hash")
            if not remote_h:
                return False
            remote_h = remote_h.decode() if isinstance(remote_h, bytes) else remote_h
            h_file = RESULT_DIR / ".hash"
            local_h = (
                (await asyncio.to_thread(h_file.read_text)).strip()
                if h_file.exists()
                else None
            )

            if local_h == remote_h:
                await self.sync_to_knot()
                return True

            log("REDIS", "Syncing state from Master...")
            for f in [
                "proxy.rpz",
                "deny.rpz",
                "deny2.rpz",
                "route-ips.txt",
                "route-ips-v6.txt",
            ]:
                data = await self.r.get(f"path:data:{f}")
                if data:
                    out_path = RESULT_DIR / f
                    tmp_out = out_path.with_suffix(".tmp")
                    await asyncio.to_thread(tmp_out.write_bytes, zlib.decompress(data))
                    await asyncio.to_thread(tmp_out.rename, out_path)

            keys = await self.r.keys("path:list:*")
            for k in keys:
                k_str = k.decode() if isinstance(k, bytes) else k
                rel_p = k_str.replace("path:list:", "")
                data = await self.r.get(k)
                if data:
                    out_path = WORKDIR / rel_p
                    await asyncio.to_thread(
                        out_path.parent.mkdir, parents=True, exist_ok=True
                    )
                    tmp_out = out_path.with_suffix(".tmp")
                    await asyncio.to_thread(tmp_out.write_bytes, data)
                    await asyncio.to_thread(tmp_out.rename, out_path)

            tmp_h = h_file.with_suffix(".tmp")
            await asyncio.to_thread(tmp_h.write_text, remote_h)
            await asyncio.to_thread(tmp_h.rename, h_file)
            await self.sync_to_knot()
            return True
        except Exception:
            return False

    async def run(self):
        try:
            role = self.env.get("NODE_ROLE", "solo").lower()
            my_id = socket.gethostname()
            is_master = role != "worker"

            if self.r:
                last_hb = await self.r_get("path:last_heartbeat")
                if last_hb:
                    last_hb = int(
                        last_hb.decode() if isinstance(last_hb, bytes) else last_hb
                    )
                    if int(time.time()) - last_hb > 900:
                        lock = await self.r.set(
                            "path:master_lock", my_id, nx=True, ex=3600
                        )
                        if lock:
                            is_master = True
                        else:
                            m = await self.r_get("path:master_lock")
                            if (
                                m
                                and (m.decode() if isinstance(m, bytes) else m) == my_id
                            ):
                                is_master = True

            if role == "worker" and not is_master:
                if await self.sync_from_redis():
                    return

            if is_master and role in ["master", "solo"]:
                await self.update_sources()

            new_h = await asyncio.to_thread(self.get_state_hash)
            h_file = RESULT_DIR / ".hash"
            if h_file.exists() and await asyncio.to_thread(h_file.read_text) == new_h:
                log("ENGINE", "No changes detected, skipping generation")
                await self.sync_to_knot()
                if self.r and is_master:
                    await self.sync_to_redis(new_h, my_id)
                return

            log("ENGINE", "Processing started")
            in_ips, _, _ = await asyncio.to_thread(
                self.load, ["include-ips"], is_ip=True
            )
            ex_ips, _, _ = await asyncio.to_thread(
                self.load, ["exclude-ips"], is_ip=True
            )
            limit = int(self.env.get("AGGREGATE_COUNT", 500))
            final_routes = {}
            for fn, ver in [("route-ips.txt", 4), ("route-ips-v6.txt", 6)]:
                is_v6 = ver == 6
                nets = [
                    ipaddress.ip_network(i, False)
                    for i in in_ips
                    if (":" in i) == is_v6
                ]
                ex_nets = [
                    ipaddress.ip_network(i, False)
                    for i in ex_ips
                    if (":" in i) == is_v6
                ]
                aggr = self.aggregate(nets, limit, ver)
                res_nets = sub_nets_optimized(aggr, ex_nets)
                out_path = RESULT_DIR / fn
                tmp_path = out_path.with_suffix(".tmp")
                await asyncio.to_thread(
                    tmp_path.write_text, "\n".join(map(str, res_nets))
                )
                await asyncio.to_thread(tmp_path.rename, out_path)
                final_routes[ver] = len(res_nets)

            f_cas_env = self.env.get("FILTER_CASINO") == "y"
            hosts_proxy_raw, cas_p_set, raw_p = await asyncio.to_thread(
                self.load, ["include-hosts"], f_cas=f_cas_env
            )
            hosts_ad_raw, _, raw_ad = await asyncio.to_thread(
                self.load, ["include-adblock-hosts", "rpz"]
            )
            hosts_ad_exc, _, _ = await asyncio.to_thread(
                self.load, ["exclude-adblock-hosts"]
            )
            hosts_deny2_raw, _, raw_d2 = await asyncio.to_thread(self.load, ["rpz2"])
            ex_proxy_only, _, _ = await asyncio.to_thread(self.load, ["exclude-hosts"])
            ex_global, _, _ = await asyncio.to_thread(self.load, ["remove-hosts"])

            def strip_prefixes(domains):
                res = set()
                for d in domains:
                    res.add(PREFIX_RE.sub("", d) if d.count(".") >= 2 else d)
                return res

            ex_common = {d for d, ex in ex_global}
            proxy_inc = {d for d, ex in hosts_proxy_raw if not ex}
            proxy_exc_raw = (
                ex_common
                | {d for d, ex in ex_proxy_only}
                | {d for d, ex in hosts_proxy_raw if ex}
            )
            p_inc_s, p_exc_s = strip_prefixes(proxy_inc), strip_prefixes(proxy_exc_raw)
            proxy_domains = [
                d for d in optimize_trie(p_inc_s | p_exc_s) if d not in p_exc_s
            ]
            proxy_exc = optimize_trie(p_exc_s)

            all_ad_rules = hosts_ad_raw | hosts_ad_exc
            ad_inc, ad_exc_ext, ad_int_ex = (
                {d for d, ex in all_ad_rules if not ex},
                {d for d, ex in hosts_ad_exc if ex},
                {d for d, ex in hosts_ad_raw if ex},
            )
            ad_final = sorted(list(ad_inc - ad_exc_ext - ad_int_ex))

            deny2_inc, deny2_exc = (
                {d for d, ex in hosts_deny2_raw if not ex},
                {d for d, ex in hosts_deny2_raw if ex},
            )
            deny2_final = sorted(list(deny2_inc - deny2_exc))

            async def write_rpz(
                name, domains, excluded_domains=None, raw_rules=None, ra=False
            ):
                tmp_path = RESULT_DIR / f"{name}.rpz.tmp"

                def _write():
                    with open(tmp_path, "w") as f:
                        f.write("$TTL 10800\n@ SOA . . (1 1 1 1 10800)\n")
                        if ra and name == "proxy":
                            f.write("* CNAME .\n")
                        if raw_rules:
                            for r in sorted(list(raw_rules)):
                                f.write(f"{r}\n")
                        if excluded_domains:
                            for d in sorted(list(excluded_domains)):
                                f.write(
                                    f"{d}. CNAME rpz-passthru.\n*.{d}. CNAME rpz-passthru.\n"
                                )
                        for d in sorted(domains):
                            if d != ".":
                                f.write(f"{d}. CNAME .\n*.{d}. CNAME .\n")

                await asyncio.to_thread(_write)
                await asyncio.to_thread(tmp_path.rename, RESULT_DIR / f"{name}.rpz")

            await write_rpz(
                "proxy",
                proxy_domains,
                proxy_exc,
                raw_p,
                self.env.get("ROUTE_ALL") == "y",
            )
            await write_rpz("deny", ad_final, ad_exc_ext | ad_int_ex, raw_ad)
            await write_rpz("deny2", deny2_final, deny2_exc, raw_d2)

            tmp_h = h_file.with_suffix(".tmp")
            await asyncio.to_thread(tmp_h.write_text, new_h)
            await asyncio.to_thread(tmp_h.rename, h_file)

            await self.sync_to_knot()
            if self.r and is_master:
                await self.sync_to_redis(new_h, my_id)
            log("ENGINE", "=============================================")
            log("ENGINE", f" IPv4 Routes:    {final_routes.get(4, 0)}")
            log("ENGINE", f" IPv6 Routes:    {final_routes.get(6, 0)}")
            log("ENGINE", "---------------------------------------------")
            log("ENGINE", " Proxy:")
            log("ENGINE", f"   Included:     {len(proxy_inc)}")
            log("ENGINE", f"   Excluded:     {len(proxy_exc_raw)}")
            log("ENGINE", f"   Result:       {len(proxy_domains)}")
            log("ENGINE", "---------------------------------------------")
            log("ENGINE", " AdBlock:")
            log("ENGINE", f"   Included:     {len(ad_inc)}")
            log("ENGINE", f"   Excluded:     {len(ad_exc_ext)}")
            log("ENGINE", f"   Internal Ex:  {len(ad_int_ex)}")
            log("ENGINE", f"   Result:       {len(ad_final)}")
            log("ENGINE", "---------------------------------------------")
            log("ENGINE", f" Global Remove:  {len(ex_common)}")
            log("ENGINE", f" Casino Filter:  {len(cas_p_set)}")
            log("ENGINE", "=============================================")
            log("ENGINE", "Status: SUCCESS")
        except Exception:
            log("ENGINE", f"CRITICAL CRASH: {traceback.format_exc()}", "ERROR")
            sys.exit(1)
        finally:
            if self.r:
                try:
                    await self.r.close()
                except Exception:
                    pass


if __name__ == "__main__":
    import fcntl

    lock_file = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        env_file, env = WORKDIR / ".env", {}
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if "=" in line:
                    k, v = line.strip().split("=", 1)
                    env[k] = v
        asyncio.run(Processor(env).run())
        sys.exit(0)
    except OSError:
        log("ENGINE", "Another instance is already running", "WARNING")
    finally:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_UN)
            lock_file.close()
        except Exception:
            pass
