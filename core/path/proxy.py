#!/usr/bin/env -S python3 -u

import asyncio
import socket
import time
import argparse
import traceback
import json
from ipaddress import ip_address, IPv4Network, IPv6Network
from collections import deque, OrderedDict
from dnslib import DNSRecord, QTYPE, A, AAAA

import redis.asyncio as redis
from config import config

CLEANUP_INTERVAL = 1800
CLEANUP_EXPIRY = 7200


class IPPool:
    def __init__(self, network, count):
        self.network = network
        self.count = count
        self.current_offset = 0
        self.recycled = deque()
        self.occupied = set()

    def popleft(self):
        if self.recycled:
            return self.recycled.popleft()
        while self.current_offset < self.count:
            ip_str = str(self.network[1 + self.current_offset])
            self.current_offset += 1
            if ip_str not in self.occupied:
                return ip_str
        return None

    def append(self, ip_str):
        self.recycled.append(ip_str)

    def set_occupied(self, occupied_set):
        self.occupied = occupied_set

    def all_ips(self):
        for i in range(self.count):
            yield str(self.network[1 + i])

    def __bool__(self):
        if self.recycled:
            return True
        if self.count - self.current_offset > len(self.occupied):
            return True
        offset = self.current_offset
        while offset < self.count:
            ip_str = str(self.network[1 + offset])
            if ip_str not in self.occupied:
                return True
            offset += 1
        return False


def log(phase, msg, status="INFO"):
    if status == "DEBUG" and not config.debug:
        return
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {f'[{status}]':9} {phase:12} | {msg}", flush=True)


class IPManager:
    def __init__(self, resolver):
        self.resolver = resolver
        self.redis_url = config.redis_url
        self.is_cluster = False
        self.r = None
        self.l1_cache_v4 = OrderedDict()
        self.l1_cache_v6 = OrderedDict()
        self.f2r_v4 = {}
        self.f2r_v6 = {}
        self._inflight = {}
        self.redis_touch_queue = set()
        self.last_seq = None

        if self.redis_url:
            try:
                params = {
                    "decode_responses": True,
                    "socket_timeout": 5,
                    "retry_on_timeout": True,
                }
                pw = config.redis_password
                if pw:
                    params["password"] = pw
                self.r = redis.from_url(self.redis_url, **params)
            except Exception as e:
                log("CLUSTER", f"Redis init failed: {e}", "WARNING")

    async def check_connection(self):
        backoff = 5
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
                    backoff = 5

                    await self.resolver.recover(silent=True)

                    seq = await self.r.get("path:sequence")
                    self.last_seq = int(seq) if seq else 0
                    await self.init_pool(
                        self.resolver.ip_pool_v4.all_ips(),
                        self.resolver.ip_pool_v6.all_ips() if self.resolver.net_v6 else []
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
                    if not any(
                        t.get_name() == "traffic_touch_worker"
                        for t in self.resolver.bg_tasks
                    ):
                        self.resolver.create_bg_task(
                            self.traffic_touch_worker(), "traffic_touch_worker"
                        )
                await asyncio.sleep(30)
            except Exception as e:
                if self.is_cluster:
                    log("CLUSTER", f"Redis connection lost: {e}", "WARNING")
                    self.is_cluster = False
                
                log("CLUSTER", f"Redis reconnecting in {backoff}s...", "DEBUG")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 300)

    async def get_fake_ip(self, real_ip, is_v6=False):
        cache = self.l1_cache_v6 if is_v6 else self.l1_cache_v4
        if real_ip in cache:
            data = cache[real_ip]
            now = time.time()
            if now - data[1] <= 7200:
                fake = data[0]
                needs_kernel_refresh = now - data[2] > 5400
                needs_redis_refresh = self.is_cluster and (now - data[3] > 1800)
                if not needs_kernel_refresh and not needs_redis_refresh:
                    if self.resolver.known_kernel_state.get(fake) == real_ip:
                        cache.move_to_end(real_ip)
                        data[1] = now
                        return fake

        async with self.resolver.lock:
            cache = self.l1_cache_v6 if is_v6 else self.l1_cache_v4
            f2r = self.f2r_v6 if is_v6 else self.f2r_v4
            if real_ip in cache:
                data = cache[real_ip]
                now = time.time()

                if now - data[1] > 7200:
                    fake = data[0]
                    cache.pop(real_ip, None)
                    f2r.pop(fake, None)
                    self.resolver.enqueue_nft(
                        ("del", "v6" if is_v6 else "v4", fake, real_ip)
                    )
                else:
                    fake = data[0]
                    cache.move_to_end(real_ip)
                    data[1] = now
                    needs_kernel_refresh = now - data[2] > 5400
                    needs_redis_refresh = self.is_cluster and (now - data[3] > 1800)

                    async with self.resolver.state_lock:
                        known_real = self.resolver.known_kernel_state.get(fake)

                    ver = "v6" if is_v6 else "v4"
                    if known_real != real_ip:
                        if known_real:
                            self.resolver.enqueue_nft(("del", ver, fake, known_real))
                        self.resolver.enqueue_nft(("add", ver, fake, real_ip))
                        data[2] = now
                    elif needs_kernel_refresh:
                        self.resolver.enqueue_nft(("del", ver, fake, real_ip))
                        self.resolver.enqueue_nft(("add", ver, fake, real_ip))
                        data[2] = now

                    if needs_redis_refresh:
                        self.redis_touch_queue.add((real_ip, fake, ver))
                        data[3] = now
                    return fake

            if real_ip in self._inflight:
                event = self._inflight[real_ip]
                is_leader = False
            else:
                event = self._inflight[real_ip] = asyncio.Event()
                is_leader = True

        if not is_leader:
            await event.wait()
            return await self.get_fake_ip(real_ip, is_v6)

        try:
            fake = None
            if self.is_cluster:
                fake = await self._get_redis(real_ip, is_v6)
                if not fake:
                    await self.init_pool(
                        self.resolver.ip_pool_v4.all_ips(),
                        self.resolver.ip_pool_v6.all_ips() if self.resolver.net_v6 else []
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
                        cache[real_ip] = [fake, now, now, now]
                        f2r[fake] = real_ip
                        if self.is_cluster and len(cache) > self.resolver.l1_limit:
                            old_real_evict, d = cache.popitem(last=False)
                            old_fake = d[0]
                            if f2r.get(old_fake) == old_real_evict:
                                del f2r[old_fake]
                            self.resolver.enqueue_nft(
                                ("del", "v6" if is_v6 else "v4", old_fake, old_real_evict)
                            )
            return fake
        finally:
            async with self.resolver.lock:
                ev = self._inflight.pop(real_ip, None)
                if ev:
                    ev.set()

    async def redis_touch_worker(self):
        lua_touch = """
        if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
            redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
            return 1
        end
        return 0
        """
        while self.resolver.running:
            try:
                await asyncio.sleep(30)
                if not self.is_cluster or not self.r or not self.redis_touch_queue:
                    continue
                async with self.resolver.lock:
                    to_touch = list(self.redis_touch_queue)
                    self.redis_touch_queue = set()
                if to_touch:
                    now = time.time()
                    for real, fake, ver in to_touch:
                        try:
                            await self.r.eval(
                                lua_touch,
                                2,
                                f"path:map:{ver}",
                                f"path:exp:{ver}",
                                real,
                                fake,
                                now,
                            )
                        except Exception:
                            continue
            except Exception as e:
                log("CLUSTER", f"Touch worker error: {e}", "WARNING")

    async def traffic_touch_worker(self):
        while self.resolver.running:
            try:
                await asyncio.sleep(300)
                if not self.is_cluster or not self.r:
                    continue

                proc = await asyncio.create_subprocess_shell(
                    "nft -j list maps inet path",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                stdout, _ = await proc.communicate()
                if proc.returncode != 0:
                    continue

                def parse_traffic_json(json_data):
                    found = {"v4_map": set(), "v6_map": set()}
                    try:
                        data = json.loads(json_data)
                        for entry in data.get("nftables", []):
                            m = entry.get("map")
                            if (
                                m
                                and m.get("table") == "path"
                                and m.get("name") in found
                            ):
                                dest = found[m["name"]]
                                for elem in m.get("elem", []):
                                    try:
                                        val = elem[0]
                                        key = (
                                            val["elem"]["val"]
                                            if isinstance(val, dict)
                                            else val
                                        )
                                        dest.add(str(key))
                                    except (IndexError, KeyError, TypeError):
                                        continue
                    except json.JSONDecodeError as e:
                        log("NFTABLES", f"Failed to parse traffic JSON: {e}", "ERROR")
                    except Exception as e:
                        log(
                            "NFTABLES",
                            f"Unexpected error in traffic parser: {e}",
                            "ERROR",
                        )
                    return found["v4_map"], found["v6_map"]

                found_v4, found_v6 = await asyncio.to_thread(parse_traffic_json, stdout)

                if found_v4 or found_v6:
                    now = time.time()
                    chunk_size = 5000
                    all_targets = [(f, "v4") for f in found_v4] + [
                        (f, "v6") for f in found_v6
                    ]
                    for i in range(0, len(all_targets), chunk_size):
                        async with self.r.pipeline() as pipe:
                            chunk = all_targets[i : i + chunk_size]
                            v4_batch = {f: now for f, v in chunk if v == "v4"}
                            v6_batch = {f: now for f, v in chunk if v == "v6"}
                            if v4_batch:
                                pipe.zadd("path:exp:v4", v4_batch)
                            if v6_batch:
                                pipe.zadd("path:exp:v6", v6_batch)
                            await pipe.execute()
                    log(
                        "CLUSTER",
                        f"Traffic touch: {len(found_v4)} v4, {len(found_v6)} v6",
                        "DEBUG",
                    )
            except Exception as e:
                log("CLUSTER", f"Traffic touch error: {e}", "WARNING")

    async def _get_fake_local(self, real_ip, is_v6=False):
        async with self.resolver.lock:
            cache = self.l1_cache_v6 if is_v6 else self.l1_cache_v4
            f2r = self.f2r_v6 if is_v6 else self.f2r_v4
            pool = self.resolver.ip_pool_v6 if is_v6 else self.resolver.ip_pool_v4
            if real_ip in cache:
                cache.move_to_end(real_ip)
                return cache[real_ip][0]
            if not pool:
                if not cache:
                    log("PROXY", "Critical: IP Pool exhausted!", "ERROR")
                    return None
                old_real = next(iter(cache.keys()))
                old_fake = cache[old_real][0]
                self.resolver.enqueue_nft(
                    ("del", "v6" if is_v6 else "v4", old_fake, old_real)
                )
                del cache[old_real]
                del f2r[old_fake]
                pool.append(old_fake)
            fake_ip = pool.popleft()
            now = time.time()
            cache[real_ip] = [fake_ip, now, now, now]
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
            local s = redis.call('INCR', 'path:sequence')
            redis.call('PUBLISH', 'path:map_new', existing .. '|' .. real_ip .. '|' .. ver .. '|' .. s)
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
            local eseq = redis.call('INCR', 'path:sequence')
            redis.call('PUBLISH', 'path:evict', fake .. '|' .. ver .. '|' .. eseq)
        end
        redis.call('HSET', m_key, real_ip, fake)
        redis.call('HSET', r_key, fake, real_ip)
        redis.call('ZADD', e_key, now, fake)
        local seq = redis.call('INCR', 'path:sequence')
        redis.call('PUBLISH', 'path:map_new', fake .. '|' .. real_ip .. '|' .. ver .. '|' .. seq)
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
                    await pubsub.subscribe("path:evict", "path:map_new")
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

                            if channel == "path:evict":
                                if isinstance(data, str) and "|" in data:
                                    parts = data.rsplit("|", 2)
                                    fake, ver = parts[0], parts[1]
                                    seq = int(parts[2]) if len(parts) == 3 else None

                                    if (
                                        seq
                                        and self.last_seq is not None
                                        and seq > self.last_seq + 1
                                    ):
                                        log(
                                            "CLUSTER",
                                            f"Sequence gap detected ({self.last_seq} -> {seq}), triggering recover",
                                            "WARNING",
                                        )
                                        await self.resolver.recover(silent=True)
                                    if seq and (
                                        self.last_seq is None or seq > self.last_seq
                                    ):
                                        self.last_seq = seq

                                    log("CLUSTER", f"Evicting {fake} ({ver})", "DEBUG")
                                    async with self.resolver.lock:
                                        f2r = (
                                            self.f2r_v6 if ver == "v6" else self.f2r_v4
                                        )
                                        cache = (
                                            self.l1_cache_v6
                                            if ver == "v6"
                                            else self.l1_cache_v4
                                        )

                                        real = f2r.pop(fake, None)
                                        if real:
                                            cache.pop(real, None)

                                        self.resolver.enqueue_nft(
                                            ("del", ver, fake, real or "unknown")
                                        )
                            elif channel == "path:map_new":
                                if isinstance(data, str) and "|" in data:
                                    parts = data.rsplit("|", 3)
                                    if len(parts) >= 3:
                                        fake, real, ver = parts[0], parts[1], parts[2]
                                        seq = int(parts[3]) if len(parts) == 4 else None

                                        if (
                                            seq
                                            and self.last_seq is not None
                                            and seq > self.last_seq + 1
                                        ):
                                            log(
                                                "CLUSTER",
                                                f"Sequence gap detected ({self.last_seq} -> {seq}), triggering recover",
                                                "WARNING",
                                            )
                                            await self.resolver.recover(silent=True)
                                        if seq and (
                                            self.last_seq is None or seq > self.last_seq
                                        ):
                                            self.last_seq = seq

                                        log(
                                            "CLUSTER",
                                            f"New mapping: {fake} -> {real} ({ver})",
                                            "DEBUG",
                                        )
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
                                            old_fake = cache.get(real, [None])[0]
                                            if old_fake and old_fake != fake:
                                                f2r.pop(old_fake, None)
                                                self.resolver.enqueue_nft(
                                                    ("del", ver, old_fake, real)
                                                )

                                            old_real = f2r.get(fake)
                                            if old_real and old_real != real:
                                                cache.pop(old_real, None)
                                                self.resolver.enqueue_nft(
                                                    ("del", ver, fake, old_real)
                                                )

                                            now = time.time()
                                            cache[real] = [fake, now, now, now]
                                            f2r[fake] = real
                                            self.resolver.enqueue_nft(
                                                ("add", ver, fake, real)
                                            )
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

                    tmp_key = f"{key}:init_tmp"
                    await self.r.delete(tmp_key)

                    chunk = []
                    for ip in pool:
                        chunk.append(ip)
                        if len(chunk) >= 5000:
                            await self.r.rpush(tmp_key, *chunk)
                            chunk = []
                    if chunk:
                        await self.r.rpush(tmp_key, *chunk)

                    await self.r.rename(tmp_key, key)
                    await self.r.set(init_flag, "1")
                finally:
                    await self.r.delete(lock_key)

            await init_pool_key("path:pool:v4", pool_v4)
            await init_pool_key("path:pool:v6", pool_v6)
        except Exception as e:
            log("CLUSTER", f"Pool initialization failed: {e}", "ERROR")

    async def expire_redis_entries(self, ver, limit):
        if not self.is_cluster:
            return

        lock_key = f"path:cleanup_lock:{ver}"
        if not await self.r.set(lock_key, "1", nx=True, ex=60):
            return

        lua_expire = """
        local exp_key, map_key, rev_key, pool_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
        local min_score, max_score, ver = ARGV[1], ARGV[2], ARGV[3]
        local expired = redis.call('ZRANGEBYSCORE', exp_key, min_score, max_score, 'LIMIT', 0, 5000)
        for _, fake in ipairs(expired) do
            local real = redis.call('HGET', rev_key, fake)
            redis.call('ZREM', exp_key, fake)
            redis.call('HDEL', rev_key, fake)
            if real then redis.call('HDEL', map_key, real) end
            redis.call('RPUSH', pool_key, fake)
            local eseq = redis.call('INCR', 'path:sequence')
            redis.call('PUBLISH', 'path:evict', fake .. '|' .. ver .. '|' .. eseq)
        end
        return #expired
        """
        try:
            max_score = time.time() - CLEANUP_EXPIRY
            total_expired = 0
            while total_expired < limit:
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
                if count <= 0:
                    break
                total_expired += count
                await asyncio.sleep(0.01)

            if total_expired > 0:
                log("CLUSTER", f"Expired {total_expired} stale mappings for {ver}")
        except Exception as e:
            log("CLUSTER", f"Redis expiry failed: {e}", "ERROR")


class PathProxyResolver:
    def __init__(
        self,
        upstream_ip="127.0.0.2",
        upstream_port=53,
    ):
        self.upstream_ip, self.upstream_port = upstream_ip, upstream_port
        self.enable_ipv6 = config.enable_ipv6
        self.role = config.node_role
        self.udp_transport = None

        f4, m4, f6, m6 = (
            config.fake_ip,
            config.fake_netmask_v4,
            config.fake_ip6,
            config.fake_netmask_v6,
        )

        self.net_v4 = IPv4Network(f"{f4}.0.0/{m4}")
        self.v4_count = min(self.net_v4.num_addresses - 2, 262144)
        self.net_v6 = IPv6Network(f"{f6}/{m6}") if self.enable_ipv6 else None
        self.v6_count = min(self.net_v6.num_addresses - 2, 262144) if self.net_v6 else 0

        self.l1_limit = max(100000, min(self.v4_count + self.v6_count, 1000000))

        self.ip_pool_v4 = IPPool(self.net_v4, self.v4_count)
        self.ip_pool_v6 = IPPool(self.net_v6, self.v6_count) if self.net_v6 else None
        self.nft_queue = asyncio.Queue(maxsize=50000)
        self.lock = asyncio.Lock()
        self.running = True
        self.ip_manager = IPManager(self)
        self.known_kernel_state = {}
        self.state_lock = asyncio.Lock()
        self.nft_exec_lock = asyncio.Lock()
        self.sem = asyncio.Semaphore(1000)
        self.active_tasks = 0
        self.bg_tasks = set()
        self._recover_scheduled = False
        self.last_full_recover = time.time()

    def _task_done(self, t):
        self.bg_tasks.discard(t)
        if not t.cancelled() and t.exception():
            log("SYSTEM", f"Task failed: {t.get_name()} -> {t.exception()}", "ERROR")

    def create_bg_task(self, coro, name):
        t = asyncio.create_task(coro, name=name)
        self.bg_tasks.add(t)
        t.add_done_callback(self._task_done)
        return t

    async def heartbeat(self):
        my_id = config.my_id
        while self.running:
            try:
                if self.ip_manager.is_cluster and self.ip_manager.r:
                    if self.role == "master":
                        await self.ip_manager.r.set("path:master_lock", my_id, ex=3600)
                        await self.ip_manager.r.set(
                            "path:last_heartbeat", int(time.time())
                        )
                    else:
                        current_lock = await self.ip_manager.r.get("path:master_lock")
                        if current_lock:
                            if isinstance(current_lock, bytes):
                                current_lock = current_lock.decode()

                            if current_lock == my_id:
                                await self.ip_manager.r.set(
                                    "path:last_heartbeat", int(time.time())
                                )
                                await self.ip_manager.r.expire("path:master_lock", 3600)
            except Exception as e:
                log("CLUSTER", f"Heartbeat error: {e}", "DEBUG")
            await asyncio.sleep(60)

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
                    self.create_bg_task(
                        self._recover_from_overflow(), "recover_overflow"
                    )
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
                proc = await asyncio.create_subprocess_exec(
                    "nft",
                    "-f",
                    "-",
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
                )
                try:
                    _, stderr = await proc.communicate(input=cmd.encode())
                except Exception:
                    if proc.returncode is None:
                        try:
                            proc.kill()
                        except Exception:
                            pass
                    raise
                err = stderr.decode().strip()

                is_missing_del = "delete" in batch[0] and "No such file" in err
                is_existing_add = "add" in batch[0] and "File exists" in err

                if proc.returncode == 0 or (
                    len(batch) == 1 and (is_missing_del or is_existing_add)
                ):
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
            log("NFTABLES", f"Batch applied ({len(lines)} commands)", "DEBUG")
        else:
            if "No such file" not in err and "File exists" not in err:
                log(
                    "NFTABLES",
                    f"Batch failed ({len(lines)} commands): {err}",
                    "WARNING",
                )

            for line in lines:
                ok_ind, err_ind = await _execute([line])
                if not ok_ind:
                    if "No such file" not in err_ind and "File exists" not in err_ind:
                        log(
                            "NFTABLES",
                            f"Command failed: {line.strip()} -> {err_ind}",
                            "ERROR",
                        )

    async def nft_worker(self):
        log("NFTABLES", "NFTables synchronizer started")
        while self.running:
            try:
                item = await self.nft_queue.get()
                items = [item]
                while not self.nft_queue.empty() and len(items) < 100:
                    items.append(self.nft_queue.get_nowait())

                try:
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
                                    f"add element inet path {ver}_map {{ {fake} : {real} }}"
                                )
                            else:
                                cmds.append(
                                    f"delete element inet path {ver}_map {{ {fake} }}"
                                )

                    if cmds:
                        await self.run_nft(cmds)
                finally:
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
            res_pkt = await self.resolve_up(packet, is_tcp)
            if not res_pkt:
                dns.header.qr, dns.header.rcode = 1, 2
                return dns.pack()
            res_dns = DNSRecord.parse(res_pkt)
            if res_dns.header.id != dns.header.id:
                raise ValueError("Transaction ID mismatch")
            res_dns.header.id = dns.header.id
            if dns.q.qtype not in (QTYPE.A, QTYPE.AAAA, 64, 65):
                return res_dns.pack()
            for section in ["rr", "auth", "ar"]:
                new_records = []
                for rr in getattr(res_dns, section):
                    if rr.rtype in (QTYPE.A, QTYPE.AAAA):
                        real_ip_raw = str(rr.rdata)
                        if real_ip_raw in ("0.0.0.0", "::"):
                            new_records.append(rr)
                            continue

                        try:
                            real_ip = str(ip_address(real_ip_raw))
                        except ValueError:
                            continue

                        fake_ip = await self.ip_manager.get_fake_ip(
                            real_ip, rr.rtype == QTYPE.AAAA
                        )
                        if fake_ip:
                            rr.rdata = (
                                A(fake_ip) if rr.rtype == QTYPE.A else AAAA(fake_ip)
                            )
                            rr.ttl = min(rr.ttl, 600)
                    elif rr.rtype in (64, 65):
                        if hasattr(rr.rdata, "params"):
                            new_params = []
                            for k, v in rr.rdata.params:
                                if k == 4:  # ipv4hint
                                    new_val = bytearray()
                                    for i in range(0, len(v), 4):
                                        chunk = v[i:i+4]
                                        if len(chunk) == 4:
                                            real_ip = socket.inet_ntoa(chunk)
                                            fake_ip = await self.ip_manager.get_fake_ip(
                                                real_ip, is_v6=False
                                            )
                                            if fake_ip:
                                                new_val.extend(socket.inet_aton(fake_ip))
                                            else:
                                                new_val.extend(chunk)
                                        else:
                                            new_val.extend(chunk)
                                    new_params.append((k, new_val))
                                elif k == 6:  # ipv6hint
                                    new_val = bytearray()
                                    for i in range(0, len(v), 16):
                                        chunk = v[i:i+16]
                                        if len(chunk) == 16:
                                            real_ip = socket.inet_ntop(socket.AF_INET6, chunk)
                                            fake_ip = await self.ip_manager.get_fake_ip(
                                                real_ip, is_v6=True
                                            )
                                            if fake_ip:
                                                new_val.extend(socket.inet_pton(socket.AF_INET6, fake_ip))
                                            else:
                                                new_val.extend(chunk)
                                        else:
                                            new_val.extend(chunk)
                                    new_params.append((k, new_val))
                                else:
                                    new_params.append((k, v))
                            rr.rdata.params = new_params
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
                try:
                    w.write(int.to_bytes(len(data), 2, "big") + data)
                    await w.drain()
                    res_len = int.from_bytes(
                        await asyncio.wait_for(r.readexactly(2), timeout=3.0), "big"
                    )
                    res = await asyncio.wait_for(r.readexactly(res_len), timeout=3.0)
                    return res
                finally:
                    try:
                        w.close()
                        await w.wait_closed()
                    except Exception:
                        pass
            else:
                family = socket.AF_INET6 if ":" in self.upstream_ip else socket.AF_INET
                sock = socket.socket(family, socket.SOCK_DGRAM)
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

    async def garbage_collector(self):
        my_id = config.my_id
        while self.running:
            try:
                await asyncio.sleep(CLEANUP_INTERVAL)
                mgr = self.ip_manager
                now = time.time()

                if mgr.is_cluster:
                    is_master = self.role != "worker"
                    if not is_master and mgr.r:
                        m = await mgr.r.get("path:master_lock")
                        if m and (m.decode() if isinstance(m, bytes) else m) == my_id:
                            is_master = True

                    if is_master:
                        await mgr.expire_redis_entries("v4", self.v4_count)
                        if self.enable_ipv6:
                            await mgr.expire_redis_entries("v6", self.v6_count)

                    if now - self.last_full_recover > 3600:
                        await self.recover(silent=True)
                        self.last_full_recover = now

                to_enqueue = []
                async with self.lock:
                    for ver, cache in [
                        ("v4", mgr.l1_cache_v4),
                        ("v6", mgr.l1_cache_v6),
                    ]:
                        f2r, pool = (
                            (mgr.f2r_v6 if ver == "v6" else mgr.f2r_v4),
                            (self.ip_pool_v6 if ver == "v6" else self.ip_pool_v4),
                        )
                        to_del = [
                            r for r, d in cache.items() if now - d[1] > CLEANUP_EXPIRY
                        ]
                        for r in to_del:
                            fake = cache[r][0]
                            to_enqueue.append(("del", ver, fake, r))
                            del cache[r]
                            del f2r[fake]
                            if not mgr.is_cluster:
                                pool.append(fake)

                for item in to_enqueue:
                    self.enqueue_nft(item)
            except Exception as e:
                log("SYSTEM", f"Garbage collector error: {e}", "ERROR")

    async def recover(self, silent=True):
        if not silent:
            log("RECOVERY", "Syncing state from kernel NFTables...")

        actual_nft_v4, actual_nft_v6 = {}, {}
        try:
            proc = await asyncio.create_subprocess_shell(
                "nft -j list maps inet path",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await proc.communicate()
            if proc.returncode == 0:

                def parse_nft_json(json_data):
                    v4, v6 = {}, {}
                    try:
                        data = json.loads(json_data)
                        for entry in data.get("nftables", []):
                            if "map" in entry:
                                m = entry["map"]
                                if m.get("table") == "path" and m.get("name") in [
                                    "v4_map",
                                    "v6_map",
                                ]:
                                    ver = "v4" if m["name"] == "v4_map" else "v6"
                                    dest = v4 if ver == "v4" else v6
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
                    except Exception:
                        pass
                    return v4, v6

                actual_nft_v4, actual_nft_v6 = await asyncio.to_thread(
                    parse_nft_json, out
                )
        except Exception as e:
            log("RECOVERY", f"NFT JSON parse failed: {e}", "WARNING")

        redis_sync_data = {"v4": {}, "v6": {}}
        redis_sync_success = False
        mgr = self.ip_manager
        remote_seq = None

        if mgr.is_cluster and mgr.r:
            try:
                log("RECOVERY", "Fetching state from Redis cluster...")
                seq_val = await mgr.r.get("path:sequence")
                remote_seq = int(seq_val) if seq_val else 0

                for ver in ["v4", "v6"]:
                    async for key, val in mgr.r.hscan_iter(
                        f"path:map:{ver}", count=1000
                    ):
                        redis_sync_data[ver][key] = val
                redis_sync_success = True
            except Exception as e:
                log("RECOVERY", f"Redis fetch failed: {e}", "ERROR")
                return

        all_nft_cmds = []
        async with self.lock:
            while not self.nft_queue.empty():
                try:
                    self.nft_queue.get_nowait()
                    self.nft_queue.task_done()
                except asyncio.QueueEmpty:
                    break

            if remote_seq is not None:
                self.last_seq = remote_seq

            async with self.state_lock:
                if not mgr.is_cluster:
                    self.known_kernel_state.clear()
                    mgr.l1_cache_v4.clear()
                    mgr.f2r_v4.clear()
                    mgr.l1_cache_v6.clear()
                    mgr.f2r_v6.clear()

                    for ver, actual_nft in [
                        ("v4", actual_nft_v4),
                        ("v6", actual_nft_v6),
                    ]:
                        cache = mgr.l1_cache_v6 if ver == "v6" else mgr.l1_cache_v4
                        f2r = mgr.f2r_v6 if ver == "v6" else mgr.f2r_v4
                        for fake, real in actual_nft.items():
                            now = time.time()
                            cache[real], f2r[fake] = [fake, now, now, now], real
                            self.known_kernel_state[fake] = real
                else:
                    if not redis_sync_success:
                        return

                    total_maps = len(redis_sync_data["v4"]) + len(redis_sync_data["v6"])
                    log("RECOVERY", f"Applying cluster state ({total_maps} domains)...")
                    self.known_kernel_state.clear()

                    for ver in ["v4", "v6"]:
                        redis_data = redis_sync_data.get(ver, {})
                        f2r = mgr.f2r_v6 if ver == "v6" else mgr.f2r_v4
                        cache = mgr.l1_cache_v6 if ver == "v6" else mgr.l1_cache_v4
                        nft_cur = actual_nft_v6 if ver == "v6" else actual_nft_v4

                        f2r.clear()
                        cache.clear()
                        actual_adds, actual_dels = [], []

                        for real, fake in redis_data.items():
                            now = time.time()
                            f2r[fake], cache[real] = real, [fake, now, now, now]
                            needs_add = True
                            if fake in nft_cur:
                                try:
                                    if ip_address(nft_cur[fake]) == ip_address(real):
                                        needs_add = False
                                        self.known_kernel_state[fake] = real
                                except Exception:
                                    pass

                            if needs_add:
                                actual_dels.append(
                                    f"delete element inet path {ver}_map {{ {fake} }}"
                                )
                                actual_adds.append(
                                    f"add element inet path {ver}_map {{ {fake} : {real} }}"
                                )

                        for fake, real in nft_cur.items():
                            if fake not in f2r:
                                actual_dels.append(
                                    f"delete element inet path {ver}_map {{ {fake} }}"
                                )

                        all_nft_cmds.extend(actual_dels)
                        all_nft_cmds.extend(actual_adds)

            occ_v4, occ_v6 = set(mgr.f2r_v4.keys()), set(mgr.f2r_v6.keys())
            self.ip_pool_v4 = IPPool(self.net_v4, self.v4_count)
            self.ip_pool_v4.set_occupied(occ_v4)
            if self.net_v6:
                self.ip_pool_v6 = IPPool(self.net_v6, self.v6_count)
                self.ip_pool_v6.set_occupied(occ_v6)

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
        if self.resolver.active_tasks >= 1000:
            log("UDP", f"Queue full, dropping query from {addr[0]}", "WARNING")
            return
        self.resolver.active_tasks += 1
        self.resolver.create_bg_task(self.run(data, addr), f"udp_{addr}")

    async def run(self, data, addr):
        try:
            async with self.resolver.sem:
                resp = await self.resolver.patch(data)
                if resp:
                    self.transport.sendto(resp, addr)
        except Exception as e:
            log("UDP", f"Request failed: {e}", "ERROR")
        finally:
            self.resolver.active_tasks -= 1


class TCP:
    def __init__(self, resolver):
        self.resolver = resolver
        self.sem = asyncio.Semaphore(200)

    async def handle(self, r, w):
        if self.sem.locked():
            log("TCP", "Queue full, closing session", "WARNING")
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
    parser.add_argument("--address", default=config.proxy_addr)
    parser.add_argument("--port", type=int, default=config.proxy_port)
    args = parser.parse_args()
    loop = asyncio.get_running_loop()

    resolver = PathProxyResolver()

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
        resolver.create_bg_task(resolver.heartbeat(), "heartbeat")
        await resolver.recover(silent=False)
        resolver.create_bg_task(
            resolver.ip_manager.check_connection(), "check_connection"
        )
        resolver.create_bg_task(resolver.garbage_collector(), "garbage_collector")
        if resolver.ip_manager.is_cluster:
            await resolver.ip_manager.init_pool(
                resolver.ip_pool_v4.all_ips(),
                resolver.ip_pool_v6.all_ips() if resolver.net_v6 else []
            )
            resolver.create_bg_task(
                resolver.ip_manager.listen_updates(), "listen_updates"
            )
            resolver.create_bg_task(
                resolver.ip_manager.redis_touch_worker(), "redis_touch_worker"
            )
            resolver.create_bg_task(
                resolver.ip_manager.traffic_touch_worker(), "traffic_touch_worker"
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
