#!/usr/bin/env -S python3 -u

import asyncio
import socket
import time
import argparse
import os
import json
import random
from collections import deque, OrderedDict
from ipaddress import IPv4Network, IPv6Network
from dnslib import DNSRecord, QTYPE, A, AAAA

import redis.asyncio as redis

CLEANUP_INTERVAL = 1800
CLEANUP_EXPIRY = 7200
L1_CACHE_SIZE = 10000


def log(phase, msg, status="INFO"):
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
                self.is_cluster = True
            except Exception as e:
                log("CLUSTER", f"Redis init failed: {e}", "WARNING")

    async def check_connection(self):
        if not self.is_cluster:
            return
        try:
            await self.r.ping()
            log("CLUSTER", "Connected to Redis")
        except Exception as e:
            log("CLUSTER", f"Redis connection failed: {e}", "WARNING")
            self.is_cluster = False

    async def get_fake_ip(self, real_ip, is_v6=False):
        cache = self.l1_cache_v6 if is_v6 else self.l1_cache_v4
        f2r = self.f2r_v6 if is_v6 else self.f2r_v4

        if real_ip in cache:
            data = cache[real_ip]
            fake = data["fake"]
            cache.move_to_end(real_ip)
            data["last"] = time.time()
            if self.resolver.known_kernel_state.get(fake) != real_ip:
                self.resolver.enqueue_nft(
                    ("add", "v6" if is_v6 else "v4", fake, real_ip)
                )
            return fake

        fake = None
        if self.is_cluster:
            try:
                fake = await self._get_redis(real_ip, is_v6)
                if not fake:
                    await self.init_pool(
                        list(self.resolver.ip_pool_v4), list(self.resolver.ip_pool_v6)
                    )
                    fake = await self._get_redis(real_ip, is_v6)
            except Exception:
                pass

        if not fake:
            fake = await self._get_fake_local(real_ip, is_v6)

        if fake:
            cache[real_ip] = {"fake": fake, "last": time.time()}
            f2r[fake] = real_ip
            if len(cache) > L1_CACHE_SIZE:
                old_real, d = cache.popitem(last=False)
                old_fake = d["fake"]
                if f2r.get(old_fake) == old_real:
                    self.resolver.enqueue_nft(
                        ("del", "v6" if is_v6 else "v4", old_fake, old_real)
                    )
                    del f2r[old_fake]
                    if not self.is_cluster:
                        (
                            self.resolver.ip_pool_v6
                            if is_v6
                            else self.resolver.ip_pool_v4
                        ).append(old_fake)
        return fake

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
            cache[real_ip] = {"fake": fake_ip, "last": time.time()}
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
            redis.call('PUBLISH', 'path:evict', fake .. ':' .. ver)
        end
        redis.call('HSET', m_key, real_ip, fake)
        redis.call('HSET', r_key, fake, real_ip)
        redis.call('ZADD', e_key, now, fake)
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
                if self.resolver.known_kernel_state.get(fake) != real_ip:
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
                    await pubsub.subscribe("path:sync", "path:evict")
                    log("CLUSTER", "Listening for cluster sync signals...")
                    while True:
                        msg = await pubsub.get_message(
                            ignore_subscribe_messages=True, timeout=60
                        )
                        if msg:
                            if msg["channel"] == "path:sync":
                                await asyncio.sleep(random.uniform(0.5, 5.0))
                                await self.resolver.recover()
                            elif msg["channel"] == "path:evict":
                                data = msg["data"]
                                if isinstance(data, str) and ":" in data:
                                    fake, ver = data.split(":", 1)
                                    f2r = self.f2r_v6 if ver == "v6" else self.f2r_v4
                                    real = f2r.pop(fake, None)
                                    if real:
                                        self.resolver.enqueue_nft(
                                            ("del", ver, fake, real)
                                        )
                                        cache = (
                                            self.l1_cache_v6
                                            if ver == "v6"
                                            else self.l1_cache_v4
                                        )
                                        cache.pop(real, None)
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
                lock_key = f"{key}:init_lock"
                if not await self.r.set(lock_key, "1", nx=True, ex=300):
                    return
                try:
                    if await self.r.exists(key):
                        return
                    chunk_size = 1000
                    for i in range(0, len(pool), chunk_size):
                        await self.r.rpush(key, *pool[i : i + chunk_size])
                finally:
                    await self.r.delete(lock_key)

            await init_pool_key("path:pool:v4", pool_v4)
            await init_pool_key("path:pool:v6", pool_v6)
        except Exception:
            pass


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
        net_v4 = IPv4Network(ip_range_v4)
        v4_count = min(net_v4.num_addresses - 2, 131070)
        self.ip_pool_v4 = deque()
        for i, addr in enumerate(net_v4.hosts()):
            if i >= v4_count:
                break
            self.ip_pool_v4.append(str(addr))
        self.ip_pool_v6 = deque()
        if self.enable_ipv6:
            net_v6 = IPv6Network(ip_range_v6)
            for i, addr in enumerate(net_v6.hosts()):
                if i >= 65535:
                    break
                self.ip_pool_v6.append(str(addr))

        self.nft_queue = asyncio.Queue(maxsize=50000)
        self.lock = asyncio.Lock()
        self.running = True
        self.ip_manager = IPManager(self, redis_url)
        self.udp_sock = None
        self.known_kernel_state = {}

    def enqueue_nft(self, item):
        try:
            self.nft_queue.put_nowait(item)
        except asyncio.QueueFull:
            pass

    async def run_nft(self, lines):
        if not lines:
            return
        cmd = "\n".join(lines) + "\n"
        proc = await asyncio.create_subprocess_shell(
            "nft -f -",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate(input=cmd.encode())
        if proc.returncode == 0:
            for line in lines:
                parts = line.split()
                if len(parts) >= 10 and parts[0] == "add":
                    self.known_kernel_state[parts[6]] = parts[8]
                elif len(parts) >= 8 and parts[0] == "delete":
                    self.known_kernel_state.pop(parts[6], None)
        else:
            for line in lines:
                p = await asyncio.create_subprocess_shell(
                    "nft -f -",
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
                )
                await p.communicate(input=(line + "\n").encode())

    async def nft_worker(self):
        log("NFTABLES", "NFTables synchronizer started")
        while self.running:
            try:
                item = await self.nft_queue.get()
                items = [item]
                while not self.nft_queue.empty() and len(items) < 100:
                    items.append(self.nft_queue.get_nowait())
                unique_adds = {}
                unique_dels = set()
                for op, ver, fake, real in items:
                    if op == "add":
                        unique_adds[(ver, fake)] = real
                    elif real:
                        unique_dels.add((ver, fake, real))
                cmds = []
                for (ver, fake), real in unique_adds.items():
                    cmds.append(
                        f"add element inet path {ver}_map {{ {fake} : {real} }}"
                    )
                for ver, fake, real in unique_dels:
                    cmds.insert(
                        0, f"delete element inet path {ver}_map {{ {fake} : {real} }}"
                    )
                if cmds:
                    await self.run_nft(cmds)
                for _ in items:
                    self.nft_queue.task_done()
            except Exception:
                pass

    async def patch(self, packet, is_tcp=False):
        try:
            dns = DNSRecord.parse(packet)
            if dns.header.qr or not dns.questions:
                return packet
            if dns.q.qtype not in (QTYPE.A, QTYPE.AAAA):
                return packet
            res_pkt = await self.resolve_up(packet, is_tcp)
            if not res_pkt:
                return packet
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
                        pool.append(fake)
            log("CLEANUP", "Old mappings purged.")

    async def recover(self):
        log("RECOVERY", "Syncing state from kernel NFTables...")
        actual_nft_v4, actual_nft_v6 = {}, {}
        try:
            proc = await asyncio.create_subprocess_shell(
                "nft -j list maps",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, _ = await proc.communicate()
            if proc.returncode == 0:
                data = json.loads(out.decode())
                for item in data.get("nftables", []):
                    if "map" in item:
                        m = item["map"]
                        if m["name"] in ("v4_map", "v6_map") and "elem" in m:
                            ver = "v4" if m["name"] == "v4_map" else "v6"
                            for e in m["elem"]:
                                fake, real = (
                                    e[0],
                                    (
                                        e[1]
                                        if isinstance(e[1], str)
                                        else e[1].get("target")
                                    ),
                                )
                                if ver == "v4":
                                    actual_nft_v4[fake] = real
                                else:
                                    actual_nft_v6[fake] = real
        except Exception:
            pass
        async with self.lock:
            mgr = self.ip_manager
            self.known_kernel_state.clear()
            if not mgr.is_cluster:
                mgr.l1_cache_v4.clear()
                mgr.f2r_v4.clear()
                mgr.l1_cache_v6.clear()
                mgr.f2r_v6.clear()
                for fake, real in actual_nft_v4.items():
                    mgr.l1_cache_v4[real], mgr.f2r_v4[fake] = (
                        {"fake": fake, "last": time.time()},
                        real,
                    )
                    self.known_kernel_state[fake] = real
                for fake, real in actual_nft_v6.items():
                    mgr.l1_cache_v6[real], mgr.f2r_v6[fake] = (
                        {"fake": fake, "last": time.time()},
                        real,
                    )
                    self.known_kernel_state[fake] = real
            else:
                log("RECOVERY", "Syncing state from Redis cluster...")
                try:
                    for ver in ["v4", "v6"]:
                        redis_data = await mgr.r.hgetall(f"path:map:{ver}")
                        f2r, cache = (
                            (mgr.f2r_v6 if ver == "v6" else mgr.f2r_v4),
                            (mgr.l1_cache_v6 if ver == "v6" else mgr.l1_cache_v4),
                        )
                        nft_cur = actual_nft_v6 if ver == "v6" else actual_nft_v4
                        f2r.clear()
                        cache.clear()
                        for real, fake in redis_data.items():
                            f2r[fake], cache[real] = (
                                real,
                                {"fake": fake, "last": time.time()},
                            )
                            self.known_kernel_state[fake] = real
                            if nft_cur.get(fake) != real:
                                self.enqueue_nft(("add", ver, fake, real))
                        for fake, real in nft_cur.items():
                            if fake not in f2r:
                                self.enqueue_nft(("del", ver, fake, real))
                except Exception:
                    pass
            occ_v4, occ_v6 = set(mgr.f2r_v4.keys()), set(mgr.f2r_v6.keys())
            if occ_v4:
                self.ip_pool_v4 = deque(
                    [ip for ip in self.ip_pool_v4 if ip not in occ_v4]
                )
            if occ_v6:
                self.ip_pool_v6 = deque(
                    [ip for ip in self.ip_pool_v6 if ip not in occ_v6]
                )


class UDP(asyncio.DatagramProtocol):
    def __init__(self, resolver):
        self.resolver = resolver

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        asyncio.create_task(self.run(data, addr))

    async def run(self, data, addr):
        resp = await self.resolver.patch(data)
        if resp:
            self.transport.sendto(resp, addr)


class TCP:
    def __init__(self, resolver):
        self.resolver = resolver

    async def handle(self, r, w):
        try:
            while True:
                len_buf = await asyncio.wait_for(r.readexactly(2), timeout=5.0)
                pkt_len = int.from_bytes(len_buf, "big")
                data = await asyncio.wait_for(r.readexactly(pkt_len), timeout=5.0)
                resp = await self.resolver.patch(data, is_tcp=True)
                if resp:
                    w.write(int.to_bytes(len(resp), 2, "big") + resp)
                    await asyncio.wait_for(w.drain(), timeout=5.0)
        except Exception:
            pass
        finally:
            w.close()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", default=os.getenv("PROXY_ADDR", "127.0.0.3"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PROXY_PORT", 53)))
    args = parser.parse_args()
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
    asyncio.create_task(resolver.ip_manager.check_connection())
    asyncio.create_task(resolver.recover())
    asyncio.create_task(resolver.nft_worker())
    asyncio.create_task(resolver.cleanup())
    if resolver.ip_manager.is_cluster:
        await resolver.ip_manager.init_pool(
            list(resolver.ip_pool_v4), list(resolver.ip_pool_v6)
        )
        asyncio.create_task(resolver.ip_manager.listen_updates())
    loop = asyncio.get_running_loop()
    await loop.create_datagram_endpoint(
        lambda: UDP(resolver), local_addr=(args.address, args.port)
    )
    t_server = await asyncio.start_server(TCP(resolver).handle, args.address, args.port)
    log("SYSTEM", f"PATH Proxy engine active on {args.address}:{args.port}")
    async with t_server:
        await t_server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
