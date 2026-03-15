# PATH (Policy-Aware Traffic Handler)

PATH is an industrial-grade, asynchronous DNS-based traffic routing and filtering system. It leverages Python 3.12 (asyncio), Knot Resolver, and nftables to implement transparent Fake-IP routing at scale.

It redirects traffic for selected domains through a gateway without distributing large routing tables to clients.

---

## Quick Start (Docker Compose)

```bash
mkdir -p /opt/path && cd /opt/path
wget https://raw.githubusercontent.com/QuasyStellar/PATH/main/docker-compose.yml

docker compose up -d
```

---

## Deployment Modes

Standalone (solo). Suitable for a single server. DNS zones and mappings are managed locally. Optional Redis can be used for persistence.

Cluster (Docker Swarm). Distributed setup for multiple nodes.

Master node responsibilities: list retrieval, Suffix Trie generation, RPZ compilation, and pushing state to Redis.

Worker node responsibilities: pulling state from Redis, subscribing to updates, serving DNS with a local L1 cache.

In a cluster, list sources and manual lists are synced from the master to workers via Redis to keep failover consistent.

---

## Installation

### Method 1: Standalone (Docker Compose)

1. Create the working directory and retrieve the configuration.
```bash
mkdir -p /opt/path && cd /opt/path
wget https://raw.githubusercontent.com/QuasyStellar/PATH/main/docker-compose.yml
```

2. Launch.
```bash
docker compose up -d
```

### Method 2: Cluster (Docker Swarm)

Phase A: Master Node (Manager) setup.

1. Prepare the environment.
```bash
mkdir -p /opt/path && cd /opt/path
wget https://raw.githubusercontent.com/QuasyStellar/PATH/main/docker-stack.yml
```

2. Edit `docker-stack.yml` and replace all `REPLACE_ME` placeholders with your Redis password.

3. Initialize the Swarm cluster.
```bash
docker swarm init --advertise-addr <MANAGER_IP>
```

4. Deploy the stack.
```bash
docker stack deploy -c docker-stack.yml path
```

Phase B: Worker Node setup.

1. SSH into the worker node.

2. Run the `docker swarm join` command shown by the manager.

3. The manager will deploy and synchronize the `worker` service automatically.

---

## Security and Operational Requirements

Requires `privileged: true` and `network_mode: host` to manage nftables and apply sysctl tuning.

If you set `PUBLIC_DNS=y`, the resolver will listen on the external interface. Only do this if you understand the exposure and have proper firewalling.

---

## Configuration Reference

Parameters are defined via environment variables in the YAML configuration files.

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ROLE` | `solo` | Node behavior: `solo`, `master`, or `worker`. |
| `REDIS_URL` | - | Redis connection string (e.g., `redis://127.0.0.1:6379`). |
| `REDIS_PASSWORD`| - | Required password for Redis authentication. |
| `PATH_DNS` | `1` | Upstream DNS selection (1-6). See `PATH_DNS Sets` below. |
| `ROUTE_ALL` | `n` | If `y`, proxies ALL traffic except `exclude-hosts`. |
| `BLOCK_ADS` | `y` | Enable/Disable Adblock filtering (RPZ). |
| `FILTER_CASINO` | `y` | Aggressively strip gambling domains from all lists. |
| `ENABLE_IPV6` | `y` | Enable dual-stack IPv6 support (DNS and routing). |
| `PUBLIC_DNS` | `n` | Allow DNS to listen on external IP. |
| `AGGREGATE_COUNT`| `500` | Target limit for the number of IP prefixes in nftables. |
| `IP` | `10` | Base IPv4 prefix for the local gateway. Example: `10` becomes `10.77.77.77`. |
| `EXTERNAL_IP` | - | External IP of the server (auto-detected if empty). |
| `FAKE_IP` | `198.18`| IPv4 prefix for the Fake-IP pool. |
| `FAKE_NETMASK_V4`| `15` | CIDR mask for IPv4 Fake-IP range. |
| `FAKE_IP6` | `fd00:18::`| IPv6 prefix for the Fake-IP pool. |
| `FAKE_NETMASK_V6`| `111` | CIDR mask for IPv6 Fake-IP range. |
| `DOH_ENABLE` | `n` | Enable DNS-over-HTTPS (DoH) endpoint. |
| `DOH_PORT` | `443` | Port for the DoH service. |
| `DOH_DOMAIN` | - | Domain name for the DoH certificate (e.g., `doh.example.com`). |
| `DOH_GENERATE_CERT`| `n` | Use Certbot to generate a Let's Encrypt certificate. |
| `DOH_CERT` | - | Path to a custom SSL certificate file. |
| `DOH_KEY` | - | Path to a custom SSL private key file. |

---

### PATH_DNS Sets

| `PATH_DNS` | Description | Upstream IPs |
|-----------|-------------|--------------|
| `1` | Cloudflare+Quad9 + MSK-IX+NSDI (recommended) [*] | `62.76.76.62`, `62.76.62.76`, `195.208.4.1`, `195.208.5.1` |
| `2` | Cloudflare+Quad9 + SkyDNS [*][1] | `193.58.251.251` |
| `3` | Cloudflare+Quad9 (use if previous choice fails) | `1.1.1.1`, `1.0.0.1`, `9.9.9.10`, `149.112.112.10` |
| `4` | Comss [**] | `83.220.169.155`, `212.109.195.93`, `195.133.25.16` |
| `5` | XBox [**] | `176.99.11.77`, `80.78.247.254`, `31.192.108.180` |
| `6` | Malw [**] | `84.21.189.133`, `193.23.209.189` |

| Note | Meaning |
|------|---------|
| [*] | DNS resolvers optimized for users located in Russia. |
| [1] | Requires a SkyDNS account (Family plan) and adding this server IP in SkyDNS. |
| [**] | Enable additional proxying and hide this server IP on some internet resources. Use only if this server is geolocated in Russia or you have problems accessing some internet resources. |

---

## DNS Filtering vs Proxy Routing

DNS filtering uses RPZ zones to block or deny domains. Proxy routing resolves selected domains to Fake-IP pools and then DNATs traffic to real IPs in the kernel.

`deny.rpz` and `deny2.rpz` are for blocking. `proxy.rpz` is for Fake-IP routing.

---

## List Management

All configuration files are located in the `./lists` directory on the host machine.

Supported formats:

- Plain domains: `example.com`
- Adblock (DNS-level): `||example.com^` (contextual options like `$third-party` are ignored; only the domain is extracted)
- RPZ: `example.com CNAME .`
- IP/CIDR: `1.2.3.4` or `192.168.0.0/24`

Automatic updates run daily at 03:00. Manual trigger.

```bash
docker exec path /root/path/process.py
```

---

## Health Checks and Verification

Check nftables maps.

```bash
nft list map inet path v4_map
nft list map inet path v6_map
```

Knot Resolver stats.

```bash
docker exec path sh -c "echo 'worker.stats()' | socat -T 1 - unix-connect:/run/knot-resolver/control/1"
```

---

## Logs

- Unified container logs: `docker logs -f path`

---

## Troubleshooting

1. DNS does not respond on the local resolver.
Check that the container is running with `network_mode: host` and `privileged: true`. Verify `kresd` is running in `docker logs -f path`.

2. No routes are created in nftables maps.
Run `/root/path/process.py` manually and check for errors. Confirm `ROUTE_ALL` or your include lists are not empty.

3. `PATH_DNS=2` does not work.
SkyDNS requires account activation and adding your server IP to their panel.

4. IPv6 mapping is empty.
Ensure `ENABLE_IPV6=y` and that your host supports IPv6. Also verify `FAKE_IP6` and `FAKE_NETMASK_V6`.

5. Worker does not sync.
Check Redis connectivity from the worker and confirm `REDIS_URL` and `REDIS_PASSWORD` are correct. Look for `path:hash` in Redis.
