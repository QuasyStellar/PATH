#!/usr/bin/env -S python3 -u

import asyncio
import socket
import time
import argparse
import os
import random
import traceback
import json
from ipaddress import ip_address, IPv4Network, IPv6Network
from collections import deque, OrderedDict
from dnslib import DNSRecord, QTYPE, A, AAAA

import redis.asyncio as redis

CLEANUP_INTERVAL = 1800
CLEANUP_EXPIRY = 7200
L1_CACHE_SIZE = 100000


def log(phase, msg, status="INFO"):
    if status == "DEBUG" and os.getenv("DEBUG") != "y":
        return
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] [{status:4}] {phase:15} | {msg}", flush=True)


class IPManager:
    def __init__(self, resolver, redis_url=None):
        self.resolver = resolver
        self.redis_url = redis_url
        self.is_cluster = False
        self.r = None
        self.l1_cache_v4 = OrderedDict()
        self.l1_cache_v6 = OrderedDict()
        self.f2r_v4 = {}
        self.f2r_v6 = {}
        self._inflight = {}
        self.redis_touch_queue = set()

        if redis_url:
            try:
                params = {
                    "decode_responses": True,
                    "socket_timeout": 5,
                    "retry_on_timeout": True,
                }
                pw = os.getenv("REDIS_PASSWORD")
                if pw:
                    params["password"] = pw
                self.r = redis.from_url(redis_url, **params)
            except Exception as e:
                log("CLUSTER", f"Redis init failed: {e}", "WARNING")

    async def check_connection(self):
        while self.resolver.running:
            if not self.r:
                self.is_cluster = False
                await asyncio.sleep(60)
                continue
            try:
                await self.r.ping()
                if not self.is_cluster:
                    log("CLUSTER", "Connected to Redis cluster storage")
                    self.is_cluster = True
                    await self.resolver.recover(silent=True)
                    await self.init_pool(
                        self.resolver._all_ips_v4, self.resolver._all_ips_v6
                    )
                    if not any(
                        t.get_name() == "listen_updates" for t in self.resolver.bg_tasks
                    ):
                        self.resolver.create_bg_task(
                            self.listen_updates(), "listen_updates"
                        )
                    if not any(
                        t.get_name() == "redis_touch_worker"
                        for t in self.resolver.bg_tasks
                    ):
                        self.resolver.create_bg_task(
                            self.redis_touch_worker(), "redis_touch_worker"
                        )
            except Exception as e:
                if self.is_cluster:
                    log("CLUSTER", f"Redis connection lost: {e}", "WARNING")
                    self.is_cluster = False
            await asyncio.sleep(10)

    async def get_fake_ip(self, real_ip, is_v6=False):
        async with self.resolver.lock:
            cache = self.l1_cache_v6 if is_v6 else self.l1_cache_v4
            f2r = self.f2r_v6 if is_v6 else self.f2r_v4
            if real_ip in cache:
                data = cache[real_ip]
                fake = data["fake"]
                cache.move_to_end(real_ip)
                now = time.time()
                data["last"] = now
                needs_kernel_refresh = now - data.get("kernel_update", 0) > 5400
                needs_redis_refresh = self.is_cluster and (
                    now - data.get("redis_update", 0) > 1800
                )

                async with self.resolver.state_lock:
                    known_real = self.resolver.known_kernel_state.get(fake)

                ver = "v6" if is_v6 else "v4"
                if known_real != real_ip:
                    if known_real:
                        self.resolver.enqueue_nft(("del", ver, fake, known_real))
                    self.resolver.enqueue_nft(("add", ver, fake, real_ip))
                    data["kernel_update"] = now
                elif needs_kernel_refresh:
                    self.resolver.enqueue_nft(("del", ver, fake, real_ip))
                    self.resolver.enqueue_nft(("add", ver, fake, real_ip))
                    data["kernel_update"] = now

                if needs_redis_refresh:
                    self.redis_touch_queue.add((fake, ver))
                    data["redis_update"] = now
                return fake

            if real_ip in self._inflight:
                event = self._inflight[real_ip]
            else:
                event = self._inflight[real_ip] = asyncio.Event()
                event = None

        if event:
            await event.wait()
            return await self.get_fake_ip(real_ip, is_v6)

        try:
            fake = None
            if self.is_cluster:
                fake = await self._get_redis(real_ip, is_v6)
                if not fake:
                    await self.init_pool(
                        self.resolver._all_ips_v4, self.resolver._all_ips_v6
                    )
                    fake = await self._get_redis(real_ip, is_v6)

            if not fake:
                fake = await self._get_fake_local(real_ip, is_v6)

            if fake:
                async with self.resolver.lock:
                    cache = self.l1_cache_v6 if is_v6 else self.l1_cache_v4
                    f2r = self.f2r_v6 if is_v6 else self.f2r_v4
                    if real_ip not in cache:
                        old_real = f2r.get(fake)
                        if old_real and old_real != real_ip:
                            cache.pop(old_real, None)
                        now = time.time()
                        cache[real_ip] = {
                            "fake": fake,
                            "last": now,
                            "kernel_update": now,
                            "redis_update": now,
                        }
                        f2r[fake] = real_ip
                        if len(cache) > L1_CACHE_SIZE:
                            old_real_evict, d = cache.popitem(last=False)
                            old_fake = d["fake"]
                            if f2r.get(old_fake) == old_real_evict:
                                self.resolver.enqueue_nft(
                                    (
                                        "del",
                                        "v6" if is_v6 else "v4",
                                        old_fake,
                                        old_real_evict,
                                    )
                                )
                                del f2r[old_fake]
                                if not self.is_cluster:
                                    (
                                        self.resolver.ip_pool_v6
                                        if is_v6
                                        else self.resolver.ip_pool_v4
                                    ).append(old_fake)
            return fake
        finally:
            async with self.resolver.lock:
                ev = self._inflight.pop(real_ip, None)
                if ev:
                    ev.set()

    async def redis_touch_worker(self):
        while self.resolver.running:
            try:
                await asyncio.sleep(30)
                if not self.is_cluster or not self.r or not self.redis_touch_queue:
                    continue
                async with self.resolver.lock:
                    to_touch = list(self.redis_touch_queue)
                    self.redis_touch_queue = set()
                if to_touch:
                    async with self.r.pipeline() as pipe:
                        now = time.time()
                        for fake, ver in to_touch:
                            pipe.zadd(f"path:exp:{ver}", {fake: now})
                        await pipe.execute()
            except Exception as e:
                log("CLUSTER", f"Touch worker error: {e}", "WARNING")

    async def _get_fake_local(self, real_ip, is_v6=False):
        async with self.resolver.lock:
            cache = self.l1_cache_v6 if is_v6 else self.l1_cache_v4
            f2r = self.f2r_v6 if is_v6 else self.f2r_v4
            pool = self.resolver.ip_pool_v6 if is_v6 else self.resolver.ip_pool_v4
            if real_ip in cache:
                cache.move_to_end(real_ip)
                return cache[real_ip]["fake"]
            if not pool:
                if not cache:
                    log("PROXY", "Critical: IP Pool exhausted!", "ERROR")
                    return None
                old_real = next(iter(cache.keys()))
                old_fake = cache[old_real]["fake"]
                self.resolver.enqueue_nft(
                    ("del", "v6" if is_v6 else "v4", old_fake, old_real)
                )
                del cache[old_real]
                del f2r[old_fake]
                pool.append(old_fake)
            fake_ip = pool.popleft()
            now = time.time()
            cache[real_ip] = {
                "fake": fake_ip,
                "last": now,
                "kernel_update": now,
                "redis_update": now,
            }
            f2r[fake_ip] = real_ip
            self.resolver.enqueue_nft(
                ("add", "v6" if is_v6 else "v4", fake_ip, real_ip)
            )
            return fake_ip

    async def _get_redis(self, real_ip, is_v6=False):
        ver = "v6" if is_v6 else "v4"
        lua = """
        local m_key, r_key, p_key, e_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
        local real_ip, now, ver = ARGV[1], tonumber(ARGV[2]), ARGV[3]
        local existing = redis.call('HGET', m_key, real_ip)
        if existing then
            redis.call('ZADD', e_key, now, existing)
            return existing
        end
        local fake = redis.call('LPOP', p_key)
        if not fake then
            local oldest = redis.call('ZRANGE', e_key, 0, 0)
            if #oldest == 0 then return nil end
            fake = oldest[1]
            local old_real = redis.call('HGET', r_key, fake)
            redis.call('ZREM', e_key, fake)
            if old_real then redis.call('HDEL', m_key, old_real) end
            redis.call('HDEL', r_key, fake)
            redis.call('PUBLISH', 'path:evict', fake .. '|' .. ver)
        end
        redis.call('HSET', m_key, real_ip, fake)
        redis.call('HSET', r_key, fake, real_ip)
        redis.call('ZADD', e_key, now, fake)
        redis.call('PUBLISH', 'path:map_new', fake .. '|' .. real_ip .. '|' .. ver)
        return fake
        """
        try:
            fake = await self.r.eval(
                lua,
                4,
                f"path:map:{ver}",
                f"path:rev:{ver}",
                f"path:pool:{ver}",
                f"path:exp:{ver}",
                real_ip,
                time.time(),
                ver,
            )
            if fake:
                async with self.resolver.state_lock:
                    known_real = self.resolver.known_kernel_state.get(fake)
                if known_real != real_ip:
                    if known_real:
                        self.resolver.enqueue_nft(("del", ver, fake, known_real))
                    self.resolver.enqueue_nft(("add", ver, fake, real_ip))
            return fake
        except Exception as e:
            log("CLUSTER", f"Lua failed: {e}", "ERROR")
            return None

    async def listen_updates(self):
        if not self.is_cluster:
            return
        while True:
            try:
                async with self.r.pubsub() as pubsub:
                    await pubsub.subscribe("path:sync", "path:evict", "path:map_new")
                    log("CLUSTER", "Listening for cluster sync signals...")
                    while True:
                        msg = await pubsub.get_message(
                            ignore_subscribe_messages=True, timeout=1.0
                        )
                        if msg:
                            channel = msg["channel"]
                            data = msg["data"]
                            if isinstance(channel, bytes):
                                channel = channel.decode()
                            if isinstance(data, bytes):
                                data = data.decode()
                            
                            if channel == "path:sync":
                                await asyncio.sleep(random.uniform(0.5, 5.0))
                                await self.resolver.recover(silent=True)
                            elif channel == "path:evict":
                                if isinstance(data, str) and "|" in data:
                                    fake, ver = data.rsplit("|", 1)
                                    async with self.resolver.lock:
                                        f2r = self.f2r_v6 if ver == "v6" else self.f2r_v4
                                        cache = self.l1_cache_v6 if ver == "v6" else self.l1_cache_v4
                                        
                                        real = f2r.pop(fake, None)
                                        if real:
                                            cache.pop(real, None)
                                            self.resolver.enqueue_nft(("del", ver, fake, real))
                                        else:
                                            self.resolver.enqueue_nft(("del", ver, fake, "unknown"))
                            elif channel == "path:map_new":
                                if isinstance(data, str) and "|" in data:
                                    parts = data.rsplit("|", 2)
                                    if len(parts) == 3:
                                        fake, real, ver = parts
                                        async with self.resolver.lock:
                                            f2r = (
                                                self.f2r_v6
                                                if ver == "v6"
                                                else self.f2r_v4
                                            )
                                            cache = (
                                                self.l1_cache_v6
                                                if ver == "v6"
                                                else self.l1_cache_v4
                                            )
                                            if real not in cache:
                                                old_real = f2r.get(fake)
                                                if old_real and old_real != real:
                                                    cache.pop(old_real, None)
                                                now = time.time()
                                                cache[real] = {"fake": fake, "last": now, "kernel_update": now, "redis_update": now}
                                                f2r[fake] = real
                                            
                                            async with self.resolver.state_lock:
                                                known_real = self.resolver.known_kernel_state.get(fake)
                                            if known_real != real:
                                                if known_real:
                                                    self.resolver.enqueue_nft(("del", ver, fake, known_real))
                                                self.resolver.enqueue_nft(("add", ver, fake, real))
                        else:
                            await asyncio.sleep(0.1)
            except Exception as e:
                log("CLUSTER", f"Subscription lost: {e}. Reconnecting...", "WARNING")
                await asyncio.sleep(5)

    async def init_pool(self, pool_v4, pool_v6):
        if not self.is_cluster:
            return
        try:

            async def init_pool_key(key, pool):
                if not pool:
                    return
                init_flag = f"{key}:ready"
                if await self.r.exists(init_flag):
                    return
                lock_key = f"{key}:init_lock"
                if not await self.r.set(lock_key, "1", nx=True, ex=300):
                    return
                try:
                    if await self.r.exists(init_flag):
                        return
                    log("CLUSTER", f"Initializing Redis pool {key}...")
                    chunk_size = 1000
                    for i in range(0, len(pool), chunk_size):
                        await self.r.rpush(key, *pool[i : i + chunk_size])
                    await self.r.set(init_flag, "1")
                finally:
                    await self.r.delete(lock_key)

            await init_pool_key("path:pool:v4", pool_v4)
            await init_pool_key("path:pool:v6", pool_v6)
        except Exception as e:
            log("CLUSTER", f"Pool initialization failed: {e}", "ERROR")

    async def expire_redis_entries(self, ver):
        if not self.is_cluster:
            return
        lua_expire = """
        local exp_key, map_key, rev_key, pool_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
        local min_score, max_score, ver = ARGV[1], ARGV[2], ARGV[3]
        local expired = redis.call('ZRANGEBYSCORE', exp_key, min_score, max_score, 'LIMIT', 0, 1000)
        for _, fake in ipairs(expired) do
            local real = redis.call('HGET', rev_key, fake)
            redis.call('ZREM', exp_key, fake)
            redis.call('HDEL', rev_key, fake)
            if real then redis.call('HDEL', map_key, real) end
            redis.call('RPUSH', pool_key, fake)
            redis.call('PUBLISH', 'path:evict', fake .. '|' .. ver)
        end
        return #expired
        """
        try:
            max_score = time.time() - CLEANUP_EXPIRY
            count = await self.r.eval(
                lua_expire,
                4,
                f"path:exp:{ver}",
                f"path:map:{ver}",
                f"path:rev:{ver}",
                f"path:pool:{ver}",
                0,
                max_score,
                ver,
            )
            if count > 0:
                log("CLUSTER", f"Expired {count} stale mappings for {ver}")
        except Exception as e:
            log("CLUSTER", f"Redis expiry failed: {e}", "ERROR")


class PathProxyResolver:
    def __init__(
        self,
        upstream_ip="127.0.0.2",
        upstream_port=53,
        enable_ipv6=False,
        ip_range_v4="198.18.0.0/15",
        ip_range_v6="fd00:18::/111",
        redis_url=None,
    ):
        self.upstream_ip, self.upstream_port = upstream_ip, upstream_port
        self.enable_ipv6 = enable_ipv6
        self.udp_transport = None
        
        self.net_v4 = IPv4Network(ip_range_v4)
        self.v4_count = min(self.net_v4.num_addresses - 2, 131070)
        self.net_v6 = IPv6Network(ip_range_v6) if self.enable_ipv6 else None
        self.v6_count = 65535 if self.enable_ipv6 else 0
        
        self._all_ips_v4 = [str(addr) for i, addr in enumerate(self.net_v4.hosts()) if i < self.v4_count]
        self._all_ips_v6 = [str(addr) for i, addr in enumerate(self.net_v6.hosts()) if i < self.v6_count] if self.net_v6 else []
        
        self.ip_pool_v4 = deque(self._all_ips_v4)
        self.ip_pool_v6 = deque(self._all_ips_v6)
        self.nft_queue = asyncio.Queue(maxsize=50000)
        self.lock = asyncio.Lock()
        self.running = True
        self.ip_manager = IPManager(self, redis_url)
        self.known_kernel_state = {}
        self.state_lock = asyncio.Lock()
        self.nft_exec_lock = asyncio.Lock()
        self.sem = asyncio.Semaphore(1000)
        self.bg_tasks = set()
        self._recover_scheduled = False

    def _task_done(self, t):
        self.bg_tasks.discard(t)
        if not t.cancelled() and t.exception():
            log("SYSTEM", f"Task failed: {t.get_name()} -> {t.exception()}", "ERROR")

    def create_bg_task(self, coro, name):
        t = asyncio.create_task(coro, name=name)
        self.bg_tasks.add(t)
        t.add_done_callback(self._task_done)
        return t

    async def _recover_from_overflow(self):
        try:
            await asyncio.sleep(0.5)
            await self.recover(silent=False)
        finally:
            self._recover_scheduled = False

    def enqueue_nft(self, item):
        try:
            self.nft_queue.put_nowait(item)
        except asyncio.QueueFull:
            log("NFTABLES", "Queue full, dropping task!", "WARNING")
            if not self._recover_scheduled:
                self._recover_scheduled = True
                try:
                    asyncio.get_running_loop()
                    self.create_bg_task(self._recover_from_overflow(), "recover_overflow")
                except RuntimeError:
                    self._recover_scheduled = False
    
    async def run_nft(self, lines):
        if not lines:
            return

        async def _execute(batch):
            if not batch:
                return True, ""
            cmd = "\n".join(batch) + "\n"
            async with self.nft_exec_lock:
                proc = await asyncio.create_subprocess_shell(
                    "nft -f -",
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
                )
                _, stderr = await proc.communicate(input=cmd.encode())
                err = stderr.decode().strip()
                
                is_missing_del = "delete" in batch[0] and "No such file" in err
                is_existing_add = "add" in batch[0] and "File exists" in err
                
                if proc.returncode == 0 or (len(batch) == 1 and (is_missing_del or is_existing_add)):
                    async with self.state_lock:
                        for line in batch:
                            try:
                                if " : " in line and (
                                    line.startswith("add") or line.startswith("replace")
                                ):
                                    left, right = line.split(" : ", 1)
                                    f = left.split("{")[-1].split()[0].strip()
                                    r = right.split("}")[0].split()[0].strip()
                                    self.known_kernel_state[f] = r
                                elif "delete" in line:
                                    f = line.split("{")[-1].split("}")[0].strip()
                                    self.known_kernel_state.pop(f, None)
                            except Exception:
                                continue
                    return True, ""
                return False, err

        ok, err = await _execute(lines)
        if ok:
            lvl = "DEBUG" if len(lines) <= 2 else "INFO"
            log("NFTABLES", f"Batch applied ({len(lines)} commands)", lvl)
        else:
            if "No such file" not in err and "File exists" not in err:
                log("NFTABLES", f"Batch failed ({len(lines)} commands): {err}", "WARNING")
            
            for line in lines:
                ok_ind, err_ind = await _execute([line])
                if not ok_ind:
                    if "No such file" not in err_ind and "File exists" not in err_ind:
                        log("NFTABLES", f"Command failed: {line.strip()} -> {err_ind}", "ERROR")

    async def nft_worker(self):
        log("NFTABLES", "NFTables synchronizer started")
        while self.running:
            try:
                item = await self.nft_queue.get()
                items = [item]
                while not self.nft_queue.empty() and len(items) < 100:
                    items.append(self.nft_queue.get_nowait())

                final_ops = {}
                for op, ver, fake, real in items:
                    final_ops[(ver, fake)] = (op, real)

                cmds = []
                async with self.state_lock:
                    for (ver, fake), (op, real) in final_ops.items():
                        in_kernel = fake in self.known_kernel_state
                        if op == "add":
                            if in_kernel:
                                cmds.append(
                                    f"delete element inet path {ver}_map {{ {fake} }}"
                                )
                            cmds.append(
                                f"add element inet path {ver}_map {{ {fake} timeout 2h : {real} }}"
                            )
                        else:
                            if in_kernel:
                                cmds.append(
                                    f"delete element inet path {ver}_map {{ {fake} }}"
                                )
                if cmds:
                    await self.run_nft(cmds)
                for _ in items:
                    self.nft_queue.task_done()
            except Exception:
                log("NFTABLES", f"Worker error: {traceback.format_exc()}", "ERROR")
                await asyncio.sleep(1)

    async def patch(self, packet, is_tcp=False):
        try:
            dns = DNSRecord.parse(packet)
            if dns.header.qr or not dns.questions:
                return packet
            if dns.q.qtype not in (QTYPE.A, QTYPE.AAAA):
                return packet
            res_pkt = await self.resolve_up(packet, is_tcp)
            if not res_pkt:
                dns.header.qr, dns.header.rcode = 1, 2
                return dns.pack()
            res_dns = DNSRecord.parse(res_pkt)
            res_dns.header.id = dns.header.id
            for section in ["rr", "auth", "ar"]:
                new_records = []
                for rr in getattr(res_dns, section):
                    if rr.rtype in (QTYPE.A, QTYPE.AAAA):
                        real_ip = str(rr.rdata)
                        fake_ip = await self.ip_manager.get_fake_ip(
                            real_ip, rr.rtype == QTYPE.AAAA
                        )
                        if fake_ip:
                            rr.rdata = (
                                A(fake_ip) if rr.rtype == QTYPE.A else AAAA(fake_ip)
                            )
                            rr.ttl = min(rr.ttl, 600)
                    new_records.append(rr)
                setattr(res_dns, section, new_records)
            return res_dns.pack()
        except Exception:
            return packet

    async def resolve_up(self, data, is_tcp=False):
        try:
            if is_tcp:
                r, w = await asyncio.wait_for(
                    asyncio.open_connection(self.upstream_ip, self.upstream_port),
                    timeout=3.0,
                )
                w.write(int.to_bytes(len(data), 2, "big") + data)
                await w.drain()
                res_len = int.from_bytes(
                    await asyncio.wait_for(r.readexactly(2), timeout=3.0), "big"
                )
                res = await asyncio.wait_for(r.readexactly(res_len), timeout=3.0)
                w.close()
                await w.wait_closed()
                return res
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.setblocking(False)
                try:
                    loop = asyncio.get_event_loop()
                    await loop.sock_sendto(
                        sock, data, (self.upstream_ip, self.upstream_port)
                    )
                    res, _ = await asyncio.wait_for(
                        loop.sock_recvfrom(sock, 65535), timeout=3.0
                    )
                    if DNSRecord.parse(res).header.tc:
                        return await self.resolve_up(data, is_tcp=True)
                    return res
                finally:
                    sock.close()
        except Exception:
            return None

    async def cleanup(self):
        while self.running:
            await asyncio.sleep(CLEANUP_INTERVAL)
            mgr = self.ip_manager
            if mgr.is_cluster:
                await mgr.expire_redis_entries("v4")
                if self.enable_ipv6:
                    await mgr.expire_redis_entries("v6")
                continue
            async with self.lock:
                now = time.time()
                for ver, cache in [("v4", mgr.l1_cache_v4), ("v6", mgr.l1_cache_v6)]:
                    f2r, pool = (
                        (mgr.f2r_v6 if ver == "v6" else mgr.f2r_v4),
                        (self.ip_pool_v6 if ver == "v6" else self.ip_pool_v4),
                    )
                    to_del = [
                        r for r, d in cache.items() if now - d["last"] > CLEANUP_EXPIRY
                    ]
                    for r in to_del:
                        fake = cache[r]["fake"]
                        self.enqueue_nft(("del", ver, fake, r))
                        del cache[r]
                        del f2r[fake]
                        if not mgr.is_cluster:
                            pool.append(fake)

    async def recover(self, silent=True):
        if not silent:
            log("RECOVERY", "Syncing state from kernel NFTables (JSON)...")
        actual_nft_v4, actual_nft_v6 = {}, {}
        try:
            proc = await asyncio.create_subprocess_shell(
                "nft -j list table inet path",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await proc.communicate()
            if proc.returncode == 0:
                data = json.loads(out.decode())
                for entry in data.get("nftables", []):
                    if "map" in entry:
                        m = entry["map"]
                        if m.get("table") == "path" and m.get("name") in [
                            "v4_map",
                            "v6_map",
                        ]:
                            ver = "v4" if m["name"] == "v4_map" else "v6"
                            dest = actual_nft_v4 if ver == "v4" else actual_nft_v6
                            for elem in m.get("elem", []):
                                try:
                                    raw_f = elem[0]
                                    f = (
                                        raw_f["elem"]["val"]
                                        if isinstance(raw_f, dict)
                                        else raw_f
                                    )
                                    r = elem[1]
                                    dest[str(f)] = str(r)
                                except (IndexError, KeyError, TypeError):
                                    continue
        except Exception as e:
            log("RECOVERY", f"NFT JSON parse failed: {e}", "WARNING")

        redis_sync_data, redis_sync_success, mgr = {}, False, self.ip_manager
        if mgr.is_cluster and mgr.r:
            try:
                log("RECOVERY", "Fetching state from Redis cluster...")
                for ver in ["v4", "v6"]:
                    redis_sync_data[ver] = await mgr.r.hgetall(f"path:map:{ver}")
                redis_sync_success = True
            except Exception as e:
                log("RECOVERY", f"Redis fetch failed: {e}", "ERROR")
                return
        all_nft_cmds = []
        async with self.lock:
            async with self.state_lock:
                if not mgr.is_cluster:
                    self.known_kernel_state.clear()
                    mgr.l1_cache_v4.clear()
                    mgr.f2r_v4.clear()
                    mgr.l1_cache_v6.clear()
                    mgr.f2r_v6.clear()
                    for fake, real in actual_nft_v4.items():
                        now = time.time()
                        mgr.l1_cache_v4[real], mgr.f2r_v4[fake] = (
                            {
                                "fake": fake,
                                "last": now,
                                "kernel_update": now,
                                "redis_update": now,
                            },
                            real,
                        )
                        self.known_kernel_state[fake] = real
                    for fake, real in actual_nft_v6.items():
                        now = time.time()
                        mgr.l1_cache_v6[real], mgr.f2r_v6[fake] = (
                            {
                                "fake": fake,
                                "last": now,
                                "kernel_update": now,
                                "redis_update": now,
                            },
                            real,
                        )
                        self.known_kernel_state[fake] = real
                else:
                    if not redis_sync_success:
                        return
                    log("RECOVERY", "Applying cluster state...")
                    self.known_kernel_state.clear()
                    for ver in ["v4", "v6"]:
                        redis_data = redis_sync_data.get(ver, {})
                        f2r, cache = (
                            (mgr.f2r_v6 if ver == "v6" else mgr.f2r_v4),
                            (mgr.l1_cache_v6 if ver == "v6" else mgr.l1_cache_v4),
                        )
                        nft_cur = actual_nft_v6 if ver == "v6" else actual_nft_v4
                        f2r.clear()
                        cache.clear()
                        actual_adds, actual_dels = [], []
                        for real, fake in redis_data.items():
                            now = time.time()
                            f2r[fake], cache[real] = (
                                real,
                                {
                                    "fake": fake,
                                    "last": now,
                                    "kernel_update": now,
                                    "redis_update": now,
                                },
                            )
                            needs_add = True
                            if fake in nft_cur:
                                try:
                                    if ip_address(nft_cur[fake]) == ip_address(real):
                                        needs_add = False
                                        self.known_kernel_state[fake] = real
                                except Exception:
                                    pass
                            if needs_add:
                                actual_adds.append(
                                    f"add element inet path {ver}_map {{ {fake} timeout 2h : {real} }}"
                                )
                                if fake in nft_cur:
                                    actual_dels.append(
                                        f"delete element inet path {ver}_map {{ {fake} }}"
                                    )
                        for fake, real in nft_cur.items():
                            if fake not in f2r:
                                actual_dels.append(
                                    f"delete element inet path {ver}_map {{ {fake} }}"
                                )
                        all_nft_cmds.extend(actual_dels)
                        all_nft_cmds.extend(actual_adds)
            
            occ_v4, occ_v6 = set(mgr.f2r_v4.keys()), set(mgr.f2r_v6.keys())
            self.ip_pool_v4 = deque([ip for ip in self._all_ips_v4 if ip not in occ_v4])
            if self.net_v6:
                self.ip_pool_v6 = deque([ip for ip in self._all_ips_v6 if ip not in occ_v6])
        
        if all_nft_cmds:
            await self.run_nft(all_nft_cmds)

    async def serve(self, address, port):
        loop = asyncio.get_running_loop()
        self.udp_transport, _ = await loop.create_datagram_endpoint(
            lambda: UDP(self), local_addr=(address, port)
        )
        t_server = await asyncio.start_server(TCP(self).handle, address, port)
        log("SYSTEM", f"PATH Proxy engine active on {address}:{port}")
        async with t_server:
            while self.running:
                await asyncio.sleep(1)
            t_server.close()
            await t_server.wait_closed()
            if self.udp_transport:
                self.udp_transport.close()


class UDP(asyncio.DatagramProtocol):
    def __init__(self, resolver):
        self.resolver = resolver

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        if not self.resolver.sem.locked():
            self.resolver.create_bg_task(self.run(data, addr), f"udp_{addr}")

    async def run(self, data, addr):
        try:
            async with self.resolver.sem:
                resp = await self.resolver.patch(data)
                if resp:
                    self.transport.sendto(resp, addr)
        except Exception as e:
            log("UDP", f"Request failed: {e}", "ERROR")


class TCP:
    def __init__(self, resolver):
        self.resolver = resolver
        self.sem = asyncio.Semaphore(200)

    async def handle(self, r, w):
        if self.sem.locked():
            w.close()
            return
        async with self.sem:
            try:
                async with asyncio.timeout(20.0):
                    while True:
                        len_buf = await asyncio.wait_for(r.readexactly(2), timeout=5.0)
                        pkt_len = int.from_bytes(len_buf, "big")
                        data = await asyncio.wait_for(
                            r.readexactly(pkt_len), timeout=5.0
                        )
                        resp = await self.resolver.patch(data, is_tcp=True)
                        if resp:
                            w.write(int.to_bytes(len(resp), 2, "big") + resp)
                            await asyncio.wait_for(w.drain(), timeout=5.0)
            except Exception as e:
                if not isinstance(
                    e,
                    (
                        asyncio.TimeoutError,
                        ConnectionResetError,
                        asyncio.IncompleteReadError,
                        EOFError,
                    ),
                ):
                    log("TCP", f"Session failed: {e}", "ERROR")
            finally:
                try:
                    w.close()
                    await w.wait_closed()
                except Exception:
                    pass


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", default=os.getenv("PROXY_ADDR", "127.0.0.3"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PROXY_PORT", 53)))
    args = parser.parse_args()
    loop = asyncio.get_running_loop()
    f4, m4, f6, m6 = (
        os.getenv("FAKE_IP", "198.18"),
        os.getenv("FAKE_NETMASK_V4", "15"),
        os.getenv("FAKE_IP6", "fd00:18::"),
        os.getenv("FAKE_NETMASK_V6", "111"),
    )
    resolver = PathProxyResolver(
        enable_ipv6=(os.getenv("ENABLE_IPV6") == "y"),
        redis_url=os.getenv("REDIS_URL"),
        ip_range_v4=f"{f4}.0.0/{m4}",
        ip_range_v6=f"{f6}/{m6}",
    )

    def stop():
        resolver.running = False
        log("SYSTEM", "Shutting down...")
        for task in asyncio.all_tasks(loop):
            task.cancel()

    import signal

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop)
    try:
        if resolver.ip_manager.r:
            try:
                await asyncio.wait_for(resolver.ip_manager.r.ping(), timeout=2.0)
                resolver.ip_manager.is_cluster = True
                log("CLUSTER", "Connected to Redis cluster storage")
            except Exception:
                resolver.ip_manager.is_cluster = False
                log(
                    "CLUSTER",
                    "Redis not available, starting in standalone mode",
                    "WARNING",
                )
        resolver.create_bg_task(resolver.nft_worker(), "nft_worker")
        await resolver.recover(silent=False)
        resolver.create_bg_task(
            resolver.ip_manager.check_connection(), "check_connection"
        )
        resolver.create_bg_task(resolver.cleanup(), "cleanup")
        if resolver.ip_manager.is_cluster:
            await resolver.ip_manager.init_pool(
                list(resolver.ip_pool_v4), list(resolver.ip_pool_v6)
            )
            resolver.create_bg_task(
                resolver.ip_manager.listen_updates(), "listen_updates"
            )
            resolver.create_bg_task(
                resolver.ip_manager.redis_touch_worker(), "redis_touch_worker"
            )
        await resolver.serve(args.address, args.port)
    except asyncio.CancelledError:
        pass
    finally:
        resolver.running = False
        if resolver.ip_manager.r:
            await resolver.ip_manager.r.close()
        for t in list(resolver.bg_tasks):
            t.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
