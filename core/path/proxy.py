#!/usr/bin/env -S python3 -u

import asyncio
import socket
import time
import argparse
import os
import json
import signal
from collections import deque, OrderedDict
from ipaddress import IPv4Network, IPv6Network
from dnslib import DNSRecord, QTYPE, A, AAAA

CLEANUP_INTERVAL = 1800
CLEANUP_EXPIRY = 7200
MIN_TTL = 300
MAX_TTL = 3600

def log(phase, msg, status="INFO"):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] [{status:7}] {phase:15} | {msg}", flush=True)

class PathProxyResolver:
    def __init__(
        self,
        upstream_ip,
        upstream_port,
        ip_range_v4,
        ip_range_v6,
        enable_ipv6,
    ):
        self.upstream_ip, self.upstream_port = upstream_ip, upstream_port
        self.enable_ipv6 = enable_ipv6

        self.ip_pool_v4 = deque([str(x) for x in IPv4Network(ip_range_v4).hosts()])
        self.total_ips_v4 = len(self.ip_pool_v4)
        self.ip_map_v4 = OrderedDict()
        self.fake_to_real_v4 = {}

        self.ip_pool_v6 = deque()
        self.total_ips_v6 = 0
        self.ip_map_v6 = OrderedDict()
        self.fake_to_real_v6 = {}
        if self.enable_ipv6:
            self.ip_pool_v6 = deque([str(x) for x in IPv6Network(ip_range_v6).hosts()])
            self.total_ips_v6 = len(self.ip_pool_v6)

        self.inflight = {}
        self.nft_queue = asyncio.Queue()
        self.lock = asyncio.Lock()
        self.running = True
        self.tasks = set()

    def create_task(self, coro):
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task

    async def recover(self):
        log("RECOVERY", "Syncing state from kernel NFTables...")
        targets = [("v4", "v4_map")]
        if self.enable_ipv6:
            targets.append(("v6", "v6_map"))
        count = 0
        for ver, map_name in targets:
            try:
                proc = await asyncio.create_subprocess_shell(
                    f"nft -j list map inet path {map_name}",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await proc.communicate()
                if proc.returncode != 0:
                    log("RECOVERY", f"NFT list {map_name} failed: {stderr.decode()}", "WARNING")
                    continue
                data = json.loads(stdout.decode())
                current_time = time.time()
                for item in data.get("nftables", []):
                    if (
                        "map" in item
                        and item["map"]["name"] == map_name
                        and "elem" in item["map"]
                    ):
                        for elem_pair in item["map"]["elem"]:
                            fake_ip = elem_pair[0]
                            real_ip = (
                                elem_pair[1]
                                if isinstance(elem_pair[1], str)
                                else elem_pair[1].get("target")
                            )
                            pool = self.ip_pool_v4 if ver == "v4" else self.ip_pool_v6
                            mapping = self.ip_map_v4 if ver == "v4" else self.ip_map_v6
                            f2r = (
                                self.fake_to_real_v4
                                if ver == "v4"
                                else self.fake_to_real_v6
                            )
                            if fake_ip in pool:
                                pool.remove(fake_ip)
                                mapping[real_ip] = {"fake": fake_ip, "last": current_time}
                                f2r[fake_ip] = real_ip
                                count += 1
            except Exception as e:
                log("RECOVERY", f"Error during {map_name} sync: {e}", "WARNING")
        log("RECOVERY", f"Restored {count} active mappings.")

    async def nft_worker(self):
        while self.running:
            try:
                items = []
                item = await asyncio.wait_for(self.nft_queue.get(), timeout=1.0)
                items.append(item)
                while not self.nft_queue.empty() and len(items) < 50:
                    items.append(self.nft_queue.get_nowait())
                v4_add = [
                    f"{fake} : {real}"
                    for op, ver, fake, real in items
                    if op == "add" and ver == "v4"
                ]
                v4_del = [
                    fake for op, ver, fake, real in items if op == "del" and ver == "v4"
                ]
                v6_add = [
                    f"{fake} : {real}"
                    for op, ver, fake, real in items
                    if op == "add" and ver == "v6"
                ]
                v6_del = [
                    fake for op, ver, fake, real in items if op == "del" and ver == "v6"
                ]
                cmd = ""
                if v4_del:
                    cmd += f"delete element inet path v4_map {{ {', '.join(v4_del)} }}\n"
                if v4_add:
                    cmd += f"add element inet path v4_map {{ {', '.join(v4_add)} }}\n"
                if v6_del:
                    cmd += f"delete element inet path v6_map {{ {', '.join(v6_del)} }}\n"
                if v6_add:
                    cmd += f"add element inet path v6_map {{ {', '.join(v6_add)} }}\n"
                if cmd:
                    proc = await asyncio.create_subprocess_shell(
                        "nft -f -",
                        stdin=asyncio.subprocess.PIPE,
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    _, stderr = await proc.communicate(input=cmd.encode())
                    if proc.returncode != 0:
                        log(
                            "NFTABLES",
                            f"Update failed: {stderr.decode().strip()}",
                            "ERROR",
                        )
                for _ in items:
                    self.nft_queue.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                log("NFTABLES", f"Worker error: {e}", "ERROR")

    async def get_fake_ip(self, real_ip, is_v6=False):
        async with self.lock:
            mapping = self.ip_map_v6 if is_v6 else self.ip_map_v4
            pool = self.ip_pool_v6 if is_v6 else self.ip_pool_v4
            f2r = self.fake_to_real_v6 if is_v6 else self.fake_to_real_v4
            if real_ip in mapping:
                mapping.move_to_end(real_ip)
                mapping[real_ip]["last"] = time.time()
                return mapping[real_ip]["fake"]
            if not pool:
                oldest = next(iter(mapping.keys()))
                fake_ip = mapping[oldest]["fake"]
                self.nft_queue.put_nowait(
                    ("del", "v6" if is_v6 else "v4", fake_ip, oldest)
                )
                del mapping[oldest]
                del f2r[fake_ip]
                pool.append(fake_ip)
            fake_ip = pool.popleft()
            mapping[real_ip] = {"fake": fake_ip, "last": time.time()}
            mapping.move_to_end(real_ip)
            f2r[fake_ip] = real_ip
            self.nft_queue.put_nowait(("add", "v6" if is_v6 else "v4", fake_ip, real_ip))
            return fake_ip

    async def cleanup(self):
        while self.running:
            await asyncio.sleep(CLEANUP_INTERVAL)
            cands = []
            async with self.lock:
                now = time.time()
                for ver, mapping in [("v4", self.ip_map_v4), ("v6", self.ip_map_v6)]:
                    for real, d in mapping.items():
                        if now - d["last"] > CLEANUP_EXPIRY:
                            cands.append((ver, real, d["fake"]))
            for ver, real, fake in cands:
                async with self.lock:
                    mapping = self.ip_map_v4 if ver == "v4" else self.ip_map_v6
                    pool = self.ip_pool_v4 if ver == "v4" else self.ip_pool_v6
                    f2r = self.fake_to_real_v4 if ver == "v4" else self.fake_to_real_v6
                    if real in mapping and (
                        time.time() - mapping[real]["last"] > CLEANUP_EXPIRY
                    ):
                        self.nft_queue.put_nowait(("del", ver, fake, real))
                        pool.append(fake)
                        del mapping[real]
                        del f2r[fake]
            if cands:
                log("CLEANUP", f"Evicted {len(cands)} expired IP mappings.")
            
            async with self.lock:
                used_v4 = len(self.ip_map_v4)
                perc_v4 = (used_v4 / self.total_ips_v4 * 100) if self.total_ips_v4 else 0
                msg = f"Pool Usage: v4={used_v4}/{self.total_ips_v4} ({perc_v4:.1f}%)"
                if self.enable_ipv6:
                    used_v6 = len(self.ip_map_v6)
                    perc_v6 = (used_v6 / self.total_ips_v6 * 100) if self.total_ips_v6 else 0
                    msg += f", v6={used_v6}/{self.total_ips_v6} ({perc_v6:.1f}%)"
                log("MONITOR", msg)

    async def resolve_up(self, data, is_tcp=False):
        try:
            if is_tcp:
                r, w = await asyncio.wait_for(
                    asyncio.open_connection(self.upstream_ip, self.upstream_port),
                    timeout=3.0,
                )
                w.write(len(data).to_bytes(2, "big") + data)
                await w.drain()
                resp_len = int.from_bytes(await r.readexactly(2), "big")
                resp = await r.readexactly(resp_len)
                w.close()
                await w.wait_closed()
                return resp
            else:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.setblocking(False)
                await asyncio.get_event_loop().sock_sendto(
                    s, data, (self.upstream_ip, self.upstream_port)
                )
                resp, _ = await asyncio.wait_for(
                    asyncio.get_event_loop().sock_recvfrom(s, 4096), timeout=3.0
                )
                s.close()
                return resp
        except Exception:
            return None

    async def patch(self, data, is_tcp=False):
        try:
            req = DNSRecord.parse(data)
            q_key = (str(req.q.qname).lower(), req.q.qtype, is_tcp)
            async with self.lock:
                if q_key in self.inflight:
                    fut = self.inflight[q_key]
                else:
                    fut = asyncio.get_event_loop().create_future()
                    self.inflight[q_key] = fut
                    self.create_task(self._fetch(q_key, data, fut, is_tcp))
            resp = await fut
            if not resp:
                return data
            reply = DNSRecord.parse(resp)
            if req.q.qtype in (QTYPE.A, QTYPE.AAAA):
                is_v6 = req.q.qtype == QTYPE.AAAA
                if is_v6 and not self.enable_ipv6:
                    return resp
                for rr in reply.rr:
                    if rr.rtype == req.q.qtype:
                        fake = await self.get_fake_ip(str(rr.rdata), is_v6)
                        if fake:
                            rr.rdata = AAAA(fake) if is_v6 else A(fake)
                            rr.ttl = max(MIN_TTL, min(rr.ttl, MAX_TTL))
            return reply.pack()
        except Exception:
            return data

    async def _fetch(self, key, data, fut, is_tcp):
        try:
            res = await self.resolve_up(data, is_tcp)
            if not fut.done():
                fut.set_result(res)
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
        finally:
            async with self.lock:
                self.inflight.pop(key, None)


class UDP(asyncio.DatagramProtocol):
    def __init__(self, res):
        self.res = res

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        self.res.create_task(self.run(data, addr))

    async def run(self, data, addr):
        try:
            self.transport.sendto(await self.res.patch(data, False), addr)
        except Exception:
            pass


class TCP:
    def __init__(self, res):
        self.res = res

    async def handle(self, r, w):
        try:
            while True:
                len_data = await r.readexactly(2)
                if not len_data:
                    break
                resp = await self.res.patch(
                    await r.readexactly(int.from_bytes(len_data, "big")), True
                )
                w.write(len(resp).to_bytes(2, "big") + resp)
                await w.drain()
        except Exception:
            pass
        finally:
            w.close()
            await w.wait_closed()


async def stop(sig, loop, res):
    log("SYSTEM", f"Received signal {sig.name}, shutting down...")
    res.running = False
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [t.cancel() for t in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


async def main():
    def load_env(path):
        env = {}
        if not os.path.exists(path):
            return env
        with open(path) as f:
            for ln in f:
                ln = ln.strip()
                if ln and not ln.startswith("#"):
                    k, _, v = ln.partition("=")
                    env[k.strip()] = v.strip()
        return env

    env = load_env(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
    p = argparse.ArgumentParser()
    p.add_argument("--upstream", default="127.0.0.2:53")
    p.add_argument("--address", default="127.0.0.3")
    p.add_argument("--port", type=int, default=53)
    args = p.parse_args()
    u_ip, _, u_port = args.upstream.partition(":")
    res = PathProxyResolver(
        u_ip,
        int(u_port or 53),
        env.get("FAKE_IP", "198.18")
        + f".0.0/{env.get('FAKE_NETMASK_V4', '15')}",
        env.get("FAKE_IP6", "fd00:18::")
        + f"/{env.get('FAKE_NETMASK_V6', '111')}",
        env.get("ENABLE_IPV6") == "y",
    )
    loop = asyncio.get_running_loop()
    for s in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            s, lambda s=s: res.create_task(stop(s, loop, res))
        )
    await res.recover()
    res.create_task(res.cleanup())
    res.create_task(res.nft_worker())
    await loop.create_datagram_endpoint(
        lambda: UDP(res), local_addr=(args.address, args.port)
    )
    server = await asyncio.start_server(TCP(res).handle, args.address, args.port)
    log("SYSTEM", f"PATH Proxy engine active on {args.address}:{args.port}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
