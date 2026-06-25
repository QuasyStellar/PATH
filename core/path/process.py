#!/usr/bin/env -S python3 -u

import os
import sys
import time
import hashlib
import ipaddress
import asyncio
import aiohttp
import zlib
import re
import idna
import shutil
import filecmp
import traceback
import gc
from collections import Counter
from pathlib import Path
from functools import lru_cache

from config import config

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
DOMAIN_FAST_RE = re.compile(
    r"^[a-z0-9_]([a-z0-9-_]{0,61}[a-z0-9_])?(\.[a-z0-9_]([a-z0-9-_]{0,61}[a-z0-9_])?)+$",
    re.I,
)

_DEL_CHARS = str.maketrans("", "", "[]_~:/?#\\@!$&'()*+,;=")


@lru_cache(maxsize=1048576)
def _normalize_domain_candidate(line):
    if not line:
        return None
    line = line.strip().lower().translate(_DEL_CHARS).strip(".")
    if not line:
        return None
    try:
        line.encode("ascii")
    except UnicodeEncodeError:
        try:
            line = idna.encode(line).decode("ascii")
        except Exception:
            return None
    return line


def log(phase, msg, status="INFO"):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {f'[{status}]':9} {phase:12} | {msg}", flush=True)


def validate_domain(line):
    if not line:
        return None
    is_wildcard = line.startswith("*.")
    domain_part = line[2:] if is_wildcard else line
    if DOMAIN_FAST_RE.match(domain_part) and len(domain_part) <= 253:
        return line
    domain_part = _normalize_domain_candidate(domain_part)
    if not domain_part or "." not in domain_part or len(domain_part) > 253:
        return None
    labels = domain_part.split(".")
    for label in labels:
        if not label or not LABEL_RE.match(label):
            return None
    return ("*." + domain_part) if is_wildcard else domain_part


@lru_cache(maxsize=1048576)
def parse_adblock_line(line, force_exception=False):
    if not line:
        return None
    line = line.strip()
    if not line or line[0] in "![" or "##" in line or "#@#" in line:
        return None
    is_ex, domain = force_exception, None
    if line.startswith("@@||"):
        is_ex, domain = True, line[4:].partition("^")[0]
    elif line.startswith("||"):
        is_ex, domain = False, line[2:].partition("^")[0]
    elif line.startswith("@@"):
        is_ex, domain = True, line[2:].partition("^")[0]
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
            content = f.read().decode("utf-8", errors="replace")
        _search = CASINO_RE.search
        _validate = validate_domain
        _parse_ad = parse_adblock_line
        _add_ad = adblock_rules.add
        _add_cas = cas_set.add
        _add_raw = raw_rules.add
        _add_ip = res.add
        _ip_net = ipaddress.ip_network
        for line in content.splitlines():
            line = line.strip()
            if not line or line[0] in "#;":
                continue
            if is_rpz and not is_ip:
                if line[0] in "$@":
                    continue
                _add_raw(line)
                continue
            if is_domain and not is_ip:
                line = line.strip("!\"#$%&'()+,-/:;<=>?@[\\]^_`{|}~")
                if not line:
                    continue
                v = _validate(line)
                if v:
                    if f_cas and _search(v):
                        _add_cas(v)
                    else:
                        _add_ad((v, is_exclude_file))
                continue
            if line[0] in "!#[]":
                continue
            if is_ip:
                ip_part = line.partition("#")[0].strip()
                if ip_part:
                    try:
                        _ip_net(ip_part, strict=False)
                        _add_ip(ip_part)
                    except Exception:
                        pass
            else:
                parsed = _parse_ad(line, force_exception=is_exclude_file)
                if parsed:
                    v, is_ex = parsed
                    if f_cas and _search(v):
                        _add_cas(v)
                    else:
                        _add_ad((v, is_ex))
    except Exception as e:
        log("PARSER", f"File validation failed: {path} ({e})", "DEBUG")
    return (res if is_ip else adblock_rules), cas_set, raw_rules


def optimize_trie(domains):
    if not domains:
        return []
    sorted_domains = sorted(domains, key=lambda d: (d.count("."), len(d)))
    res, found_roots = [], set()
    for d in sorted_domains:
        is_sub, dot_idx = False, d.rfind(".")
        while dot_idx != -1:
            if d[dot_idx + 1 :] in found_roots:
                is_sub = True
                break
            dot_idx = d.rfind(".", 0, dot_idx)
        if not is_sub:
            res.append(d)
            found_roots.add(d)
    return res


def sub_nets_optimized(inc_nets, exc_ips):
    if not inc_nets:
        return []
    if not exc_ips:
        return sorted(inc_nets)
    
    is_v6 = inc_nets[0].version == 6
    addr_cls = ipaddress.IPv6Address if is_v6 else ipaddress.IPv4Address

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
                addr_cls(s), addr_cls(e)
            )
        )
    return final_nets


class Processor:
    def __init__(self):
        for d in [RESULT_DIR, DOWNLOAD_DIR]:
            d.mkdir(parents=True, exist_ok=True)
        self.r = None
        if config.redis_url:
            import redis.asyncio as redis

            try:
                self.r = redis.from_url(
                    config.redis_url,
                    password=config.redis_password,
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
            "ROUTE_ALL",
            "BLOCK_ADS",
            "FILTER_CASINO",
            "ENABLE_IPV6",
            "IPV6_PROXY_ONLY",
            "AGGREGATE_COUNT",
            "IP",
            "FAKE_IP",
        ]:
            h.update(f"{k}={config.get(k, '')}".encode())
        return h.hexdigest()

    async def update_sources(self):
        url_map, active_stems, stems_with_urls = {}, set(), set()
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
        for s in active_stems - stems_with_urls:
            d = DOWNLOAD_DIR / s
            if d.exists() and d.is_dir():
                await asyncio.to_thread(shutil.rmtree, d)
        if not url_map:
            for ed in DOWNLOAD_DIR.iterdir():
                if ed.is_dir() and ed.name not in active_stems and ed != TEMP_DIR:
                    await asyncio.to_thread(shutil.rmtree, ed)
            return
        td = TEMP_DIR / "downloads"
        if td.exists():
            await asyncio.to_thread(shutil.rmtree, td)
        await asyncio.to_thread(td.mkdir, parents=True)
        log("FETCHER", f"Checking updates for {len(url_map)} sources...")
        sem = asyncio.Semaphore(10)
        async with aiohttp.ClientSession() as sess:
            tasks = [self.fetch(sess, url, ps[0], sem) for url, ps in url_map.items()]
            results = await asyncio.gather(*tasks)
            for (url, ps), success in zip(url_map.items(), results):
                if success:
                    for ep in ps[1:]:
                        await asyncio.to_thread(
                            ep.parent.mkdir, parents=True, exist_ok=True
                        )
                        await asyncio.to_thread(shutil.copy, ps[0], ep)
        if td.exists():
            for nd in td.iterdir():
                if nd.is_dir() and list(nd.glob("*.txt")):
                    dd = DOWNLOAD_DIR / nd.name
                    if dd.exists():
                        await asyncio.to_thread(shutil.rmtree, dd)
                    await asyncio.to_thread(shutil.move, str(nd), str(dd))
            for ed in DOWNLOAD_DIR.iterdir():
                if ed.is_dir() and ed.name not in active_stems and ed != td.parent:
                    await asyncio.to_thread(shutil.rmtree, ed)
        if TEMP_DIR.exists():
            await asyncio.to_thread(shutil.rmtree, TEMP_DIR)

    async def fetch(self, sess, url, path, sem):
        async with sem:
            try:
                await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
                async with sess.get(url, timeout=30) as r:
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
            is_ex_f, is_rpz_f, is_ad_f = (
                name.startswith("exclude") or name.startswith("remove"),
                "rpz" in name,
                "adblock" in name,
            )
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
                o, c, r = validate_file(
                    f,
                    is_ip,
                    f_cas,
                    is_exclude_file=is_ex_f,
                    is_rpz=is_rpz_f,
                    is_domain=is_dom_f,
                )
                res.update(o)
                all_cas.update(c)
                all_raw.update(r)
        return res, all_cas, all_raw

    def aggregate(self, nets, limit, ver=4):
        if not nets:
            return []
        res = list(ipaddress.collapse_addresses(nets))
        if limit <= 0 or len(res) <= limit:
            return sorted(res)
        
        min_prefix = 12 if ver == 4 else 32
        
        while len(res) > limit:
            mp = max(n.prefixlen for n in res)
            if mp <= min_prefix:
                break
            supers = Counter(n.supernet() if n.prefixlen == mp else n for n in res)
            new_res = []
            for n in res:
                if n.prefixlen == mp and supers[n.supernet()] > 1:
                    new_res.append(n.supernet())
                else:
                    new_res.append(n)
            res = list(ipaddress.collapse_addresses(new_res))
            if len(res) > limit and max(n.prefixlen for n in res) == mp:
                res = list(
                    ipaddress.collapse_addresses(
                        [n.supernet() if n.prefixlen == mp else n for n in res]
                    )
                )
        return sorted(res)

    async def sync_to_knot(self):
        log("SYNC", "Syncing RPZ zones to DNS server...")
        changed = False
        for z in ["adblock", "deny", "deny2", "proxy"]:
            src, dst = RESULT_DIR / f"{z}.rpz", KNOT_DIR / f"{z}.rpz"
            if src.exists() and (
                not dst.exists()
                or not await asyncio.to_thread(filecmp.cmp, src, dst, shallow=False)
            ):
                tmp = dst.with_suffix(".tmp")
                await asyncio.to_thread(shutil.copy2, src, tmp)
                await asyncio.to_thread(os.chmod, tmp, 0o644)
                await asyncio.to_thread(tmp.rename, dst)
                changed = True
        if changed:
            cd = "/run/knot-resolver/control"
            if os.path.exists(cd):
                for sn in os.listdir(cd):
                    try:
                        proc = await asyncio.create_subprocess_exec(
                            "socat",
                            "-",
                            f"unix-connect:{os.path.join(cd, sn)}",
                            stdin=asyncio.subprocess.PIPE,
                            stdout=asyncio.subprocess.DEVNULL,
                            stderr=asyncio.subprocess.DEVNULL,
                        )
                        try:
                            await asyncio.wait_for(
                                proc.communicate(input=b"cache.clear()\n"),
                                timeout=5.0
                            )
                        except asyncio.TimeoutError:
                            try:
                                proc.kill()
                            except Exception:
                                pass
                            await proc.wait()
                    except Exception:
                        pass

    async def sync_to_nft(self):
        log("SYNC", "Syncing Deny-IPs to nftables...")
        for fn, sn in [("deny-ips.txt", "deny_v4"), ("deny-ips-v6.txt", "deny_v6")]:
            p = RESULT_DIR / fn
            if p.exists():
                ips = (await asyncio.to_thread(p.read_text)).strip().splitlines()
                try:
                    proc = await asyncio.create_subprocess_exec(
                        "nft",
                        "flush",
                        "set",
                        "inet",
                        "path",
                        sn,
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL,
                    )
                    await proc.wait()
                except Exception:
                    pass

                if ips:
                    tmp = Path(f"/tmp/{sn}.nft")
                    await asyncio.to_thread(
                        tmp.write_text,
                        f"add element inet path {sn} {{ {','.join(ips)} }}",
                    )
                    try:
                        proc = await asyncio.create_subprocess_exec(
                            "nft",
                            "-f",
                            str(tmp),
                            stdout=asyncio.subprocess.DEVNULL,
                            stderr=asyncio.subprocess.DEVNULL,
                        )
                        await proc.wait()
                    finally:
                        if tmp.exists():
                            await asyncio.to_thread(tmp.unlink)

    async def sync_to_redis(self, h, my_id):
        if not self.r:
            return
        try:
            payloads = {}
            for f in [
                "proxy.rpz",
                "adblock.rpz",
                "deny.rpz",
                "deny2.rpz",
                "route-ips.txt",
                "route-ips-v6.txt",
                "deny-ips.txt",
                "deny-ips-v6.txt",
            ]:
                p = RESULT_DIR / f
                if p.exists():
                    payloads[f] = zlib.compress(await asyncio.to_thread(p.read_bytes))
            lists = {}
            for pd in [SOURCE_DIR, MANUAL_DIR]:
                if pd.exists():
                    for fp in pd.glob("*.txt"):
                        lists[str(fp.relative_to(WORKDIR))] = await asyncio.to_thread(
                            fp.read_bytes
                        )
            lua = """
            local my_id, role, new_h = ARGV[1], ARGV[2], ARGV[3]
            local lock = redis.call('GET', 'path:master_lock')
            if role ~= 'master' and lock and lock ~= my_id then return {err = "LOCK_LOST"} end
            for i=4, #ARGV - 1, 2 do redis.call('SET', ARGV[i], ARGV[i+1]) end
            redis.call('SET', 'path:hash', new_h)
            redis.call('SET', 'path:last_heartbeat', ARGV[#ARGV])
            redis.call('SET', 'path:master_lock', my_id, 'EX', 3600)
            redis.call('PUBLISH', 'path:sync', 'reload')
            return "OK"
            """
            args = [my_id, config.node_role, h]
            for f, d in payloads.items():
                args.extend([f"path:data:{f}", d])
            for f, d in lists.items():
                args.extend([f"path:list:{f}", d])
            args.append(int(time.time()))
            await self.r.eval(lua, 0, *args)
        except Exception as e:
            if "LOCK_LOST" in str(e):
                log("REDIS", "Master lock lost during sync, aborting", "WARNING")
            else:
                log("REDIS", f"Sync failed: {e}", "ERROR")

    async def sync_from_redis(self):
        if not self.r:
            return False
        try:
            hs = await self.r.get("path:hash")
            if not hs:
                return False
            hs = hs.decode() if isinstance(hs, bytes) else hs
            hf = RESULT_DIR / ".hash"
            lh = (
                (await asyncio.to_thread(hf.read_text)).strip() if hf.exists() else None
            )
            if lh == hs:
                await self.sync_to_knot()
                await self.sync_to_nft()
                return True
            log("REDIS", "Syncing state from Master...")
            fts = [
                "proxy.rpz",
                "adblock.rpz",
                "deny.rpz",
                "deny2.rpz",
                "route-ips.txt",
                "route-ips-v6.txt",
                "deny-ips.txt",
                "deny-ips-v6.txt",
            ]
            rd = await self.r.mget([f"path:data:{f}" for f in fts])
            he = await self.r.get("path:hash")
            he = he.decode() if isinstance(he, bytes) else he
            if hs != he:
                return await self.sync_from_redis()
            for i, f in enumerate(fts):
                d = rd[i]
                if d:
                    p = RESULT_DIR / f
                    tmp = p.with_suffix(".tmp")
                    await asyncio.to_thread(tmp.write_bytes, zlib.decompress(d))
                    await asyncio.to_thread(tmp.rename, p)

            async def _save_lists(keys, data):
                for i, bk in enumerate(keys):
                    ks = bk.decode() if isinstance(bk, bytes) else bk
                    rp = ks.replace("path:list:", "")
                    if ".." in rp:
                        continue
                    p = WORKDIR / rp
                    if not p.is_relative_to(WORKDIR / "lists"):
                        continue
                    d = data[i]
                    if d:
                        await asyncio.to_thread(
                            p.parent.mkdir, parents=True, exist_ok=True
                        )
                        tmp = p.with_suffix(".tmp")
                        await asyncio.to_thread(tmp.write_bytes, d)
                        await asyncio.to_thread(tmp.rename, p)

            batch = []
            async for k in self.r.scan_iter("path:list:*", count=1000):
                batch.append(k)
                if len(batch) >= 500:
                    ld = await self.r.mget(batch)
                    await _save_lists(batch, ld)
                    batch = []
            if batch:
                ld = await self.r.mget(batch)
                await _save_lists(batch, ld)

            tmp_h = hf.with_suffix(".tmp")
            await asyncio.to_thread(tmp_h.write_text, he)
            await asyncio.to_thread(tmp_h.rename, hf)
            await self.sync_to_knot()
            await self.sync_to_nft()
            return True
        except Exception as e:
            log("REDIS", f"Failed to sync state from Redis: {e}", "ERROR")
            return False

    async def run(self):
        try:
            role, my_id = config.node_role, config.my_id
            is_master = role != "worker"
            if self.r:
                lhb = await self.r_get("path:last_heartbeat")
                if lhb:
                    lhb = int(lhb.decode() if isinstance(lhb, bytes) else lhb)
                    if int(time.time()) - lhb > 900:
                        if await self.r.set(
                            "path:master_lock", my_id, nx=True, ex=3600
                        ):
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
            if is_master:
                await self.update_sources()
            new_h = await asyncio.to_thread(self.get_state_hash)
            hf = RESULT_DIR / ".hash"
            if hf.exists() and await asyncio.to_thread(hf.read_text) == new_h:
                log("ENGINE", "No changes detected, skipping generation")
                await self.sync_to_knot()
                if self.r and is_master:
                    await self.sync_to_redis(new_h, my_id)
                return
            log("ENGINE", "Processing started")
            in_ips, _, _ = await asyncio.to_thread(self.load, ["include-ips"], is_ip=True)
            ex_ips, _, _ = await asyncio.to_thread(self.load, ["exclude-ips"], is_ip=True)
            dn_ips, _, _ = await asyncio.to_thread(self.load, ["deny-ips"], is_ip=True)
            limit, f_routes, f_deny = config.aggregate_count, {}, {}

            for fn, ver in [("deny-ips.txt", 4), ("deny-ips-v6.txt", 6)]:
                is_v6 = ver == 6
                nets = [ipaddress.ip_network(i, False) for i in dn_ips if (":" in i) == is_v6]
                ex_nets = [ipaddress.ip_network(i, False) for i in ex_ips if (":" in i) == is_v6]
                res_nets = sub_nets_optimized(nets, ex_nets)
                p = RESULT_DIR / fn
                tmp = p.with_suffix(".tmp")
                await asyncio.to_thread(tmp.write_text, "\n".join(map(str, res_nets)))
                await asyncio.to_thread(tmp.rename, p)
                f_deny[ver] = len(res_nets)

            for fn, ver in [("route-ips.txt", 4), ("route-ips-v6.txt", 6)]:
                is_v6 = ver == 6
                nets = [ipaddress.ip_network(i, False) for i in in_ips if (":" in i) == is_v6]
                ex_nets = [ipaddress.ip_network(i, False) for i in (ex_ips | dn_ips) if (":" in i) == is_v6]
                res_nets = sub_nets_optimized(nets, ex_nets)
                aggr = self.aggregate(res_nets, limit, ver)
                p = RESULT_DIR / fn
                tmp = p.with_suffix(".tmp")
                await asyncio.to_thread(tmp.write_text, "\n".join(map(str, aggr)))
                await asyncio.to_thread(tmp.rename, p)
                f_routes[ver] = len(aggr)
            fc_env = config.filter_casino
            hpr, cas_p, raw_p = await asyncio.to_thread(
                self.load, ["include-hosts"], f_cas=fc_env
            )
            har, _, raw_ad = await asyncio.to_thread(
                self.load, ["include-adblock-hosts"]
            )
            hae, _, _ = await asyncio.to_thread(self.load, ["exclude-adblock-hosts"])
            hmr, _, raw_manual = await asyncio.to_thread(self.load, ["rpz"])
            hd2r, _, raw_d2 = await asyncio.to_thread(self.load, ["rpz2"])
            ex_p_only, _, _ = await asyncio.to_thread(self.load, ["exclude-hosts"])
            ex_g, _, _ = await asyncio.to_thread(self.load, ["remove-hosts"])

            def strip_prefixes(domains):
                res = set()
                for d in domains:
                    res.add(PREFIX_RE.sub("", d) if d.count(".") >= 2 else d)
                return res

            from itertools import chain

            ex_common = {d for d, ex in ex_g}
            del ex_g
            p_inc = {d for d, ex in hpr if not ex}
            p_exc_raw = (
                ex_common | {d for d, ex in ex_p_only} | {d for d, ex in hpr if ex}
            )
            pi_s, pe_s = strip_prefixes(p_inc), strip_prefixes(p_exc_raw)
            p_doms = [d for d in optimize_trie(pi_s | pe_s) if d not in pe_s]
            p_exc = optimize_trie(pe_s)
            c_p_inc, c_p_exc = len(p_inc), len(p_exc_raw)
            del p_inc, p_exc_raw, pi_s, pe_s
            gc.collect()
            ad_inc, ad_exc_ext, ad_int_ex = set(), set(), set()
            ext_lookup = {d for d, ex in hae}
            for d, ex in chain(har, hae):
                if ex:
                    if d in ext_lookup:
                        ad_exc_ext.add(d)
                    else:
                        ad_int_ex.add(d)
                else:
                    ad_inc.add(d)
            del har, hae, ext_lookup
            ad_inc.difference_update(ad_exc_ext)
            ad_inc.difference_update(ad_int_ex)
            ad_inc.difference_update(ex_common)
            c_ad_inc, c_ad_exc, c_ad_int = len(ad_inc), len(ad_exc_ext), len(ad_int_ex)
            ad_final = sorted(list(ad_inc))
            ad_excl = ad_exc_ext | ad_int_ex
            del ad_inc, ad_exc_ext, ad_int_ex
            gc.collect()
            m_inc = {d for d, ex in hmr if not ex}
            m_exc = {d for d, ex in hmr if ex}
            del hmr
            m_inc.difference_update(m_exc)
            m_inc.difference_update(ex_common)
            m_final = sorted(list(m_inc))
            del m_inc
            gc.collect()
            d2_inc = {d for d, ex in hd2r if not ex}
            d2_exc = {d for d, ex in hd2r if ex}
            del hd2r
            d2_inc.difference_update(d2_exc)
            d2_inc.difference_update(ex_common)
            d2_final = sorted(list(d2_inc))
            del d2_inc
            gc.collect()

            async def write_rpz(name, doms, excl=None, raw=None, ra=False):
                p = RESULT_DIR / f"{name}.rpz.tmp"

                def _w():
                    with open(p, "w") as f:
                        f.write("$TTL 10800\n@ SOA . . (1 1 1 1 10800)\n")
                        if ra and name == "proxy":
                            f.write("* CNAME .\n")
                        if raw:
                            for r in sorted(list(raw)):
                                f.write(f"{r}\n")
                        if excl:
                            for d in sorted(list(excl)):
                                f.write(
                                    f"{d}. CNAME rpz-passthru.\n*.{d}. CNAME rpz-passthru.\n"
                                )
                        for d in doms:
                            if d != ".":
                                f.write(f"{d}. CNAME .\n*.{d}. CNAME .\n")

                await asyncio.to_thread(_w)
                await asyncio.to_thread(p.rename, RESULT_DIR / f"{name}.rpz")

            await write_rpz("proxy", p_doms, p_exc, raw_p, config.route_all)
            await write_rpz("adblock", ad_final, ad_excl, raw_ad)
            await write_rpz("deny", m_final, m_exc, raw_manual)
            await write_rpz("deny2", d2_final, d2_exc, raw_d2)
            tmp_h = hf.with_suffix(".tmp")
            await asyncio.to_thread(tmp_h.write_text, new_h)
            await asyncio.to_thread(tmp_h.rename, hf)
            await self.sync_to_knot()
            await self.sync_to_nft()
            if self.r and is_master:
                await self.sync_to_redis(new_h, my_id)
            log("ENGINE", "=============================================")
            log("ENGINE", f" IPv4 Routes:    {f_routes.get(4, 0)}")
            log("ENGINE", f" IPv6 Routes:    {f_routes.get(6, 0)}")
            log("ENGINE", f" IPv4 Deny:      {f_deny.get(4, 0)}")
            log("ENGINE", f" IPv6 Deny:      {f_deny.get(6, 0)}")
            log("ENGINE", "---------------------------------------------")
            log("ENGINE", " Proxy:")
            log("ENGINE", f"   Included:     {c_p_inc}")
            log("ENGINE", f"   Excluded:     {c_p_exc}")
            log("ENGINE", f"   Result:       {len(p_doms)}")
            log("ENGINE", "---------------------------------------------")
            log("ENGINE", " AdBlock:")
            log("ENGINE", f"   Included:     {c_ad_inc}")
            log("ENGINE", f"   Excluded:     {c_ad_exc}")
            log("ENGINE", f"   Internal Ex:  {c_ad_int}")
            log("ENGINE", f"   Result:       {len(ad_final)}")
            log("ENGINE", "---------------------------------------------")
            log("ENGINE", f" Global Remove:  {len(ex_common)}")
            log("ENGINE", f" Casino Filter:  {len(cas_p)}")
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
        asyncio.run(Processor().run())
        sys.exit(0)
    except OSError:
        log("ENGINE", "Another instance is already running", "WARNING")
    finally:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_UN)
            lock_file.close()
        except Exception:
            pass
