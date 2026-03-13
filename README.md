# PATH (Policy-Aware Traffic Handler)

PATH is an industrial-grade, asynchronous DNS-based traffic routing and filtering system. It leverages a modern Linux network stack comprising Python 3.12 (asyncio), Knot Resolver, and NFTables to implement transparent Fake-IP technology at scale.

The system redirects traffic destined for restricted resources through a gateway without necessitating the distribution of massive routing tables to client devices.

---

## Technical Features

- **DNS-Based Proxying (Fake-IP):** Resolves targeted domains to dedicated internal pools (`198.18.0.0/15` and `fd00:18::/111`). Redirection occurs at the kernel level via NFTables DNAT.
- **Constant-Time Operations (O(1)):**
    - **Routing Lookups:** NFTables Maps provide constant-time lookups regardless of the number of routed domains.
    - **IP Eviction:** Utilizes an O(1) LRU (Least Recently Used) algorithm for IP pool management, implemented via OrderedDict (Solo) or Lua scripting (Redis).
- **Suffix Trie Optimization:** High-efficiency domain collapsing algorithm that merges subdomains into wildcard entries (e.g., `*.google.com`), significantly reducing DNS zone size.
- **Smart Hashing & Self-Healing:**
    - **Content-Aware Hashing:** Skips list processing and Trie generation only if the actual content of the downloaded lists is unchanged.
    - **Integrity Enforcement:** Automatically detects missing or corrupted RPZ files and triggers regeneration even if the state hash matches.
- **Cluster Synchronization:** Shared state architecture across multiple nodes using Redis Pub/Sub and targeted cache invalidation.
- **Reliability Hardening:**
    - **UDP TXID Isolation:** Every query uses a unique ephemeral socket to prevent response mixing.
    - **TCP Fallback:** Automatically switches to TCP if the UDP response has the TC (Truncated) bit set.
    - **Sync Jitter:** Randomized delays in cluster updates to prevent simultaneous "Sync Storms".

---

## Deployment Modes

### 1. Standalone (Solo)
Suitable for single-server environments. Mappings and DNS zones are managed locally. Persistence can be enabled by providing a `REDIS_URL`.

### 2. Cluster (Docker Swarm)
Distributed architecture for multi-node environments.
- **Master Node:** Handles external list retrieval, Suffix Trie generation, RPZ compilation, and state distribution to Redis.
- **Worker Nodes:** Edge nodes that pull configuration from Redis, subscribe to real-time state updates, and serve client requests with sub-millisecond L1 cache.

---

## Installation

### Method 1: Standalone (Docker Compose)

1. Create the working directory and retrieve the configuration:
   ```bash
   mkdir -p /opt/path && cd /opt/path
   wget https://raw.githubusercontent.com/QuasyStellar/PATH/main/docker-compose.yml
   ```
2. Launch:
   ```bash
   docker compose up -d
   ```

### Method 2: Cluster (Docker Swarm)

#### Phase A: Master Node (Manager) Setup
1. **Prepare the environment:**
   ```bash
   mkdir -p /opt/path && cd /opt/path
   wget https://raw.githubusercontent.com/QuasyStellar/PATH/main/docker-stack.yml
   ```
2. **Configuration:** Edit `docker-stack.yml` and replace all `REPLACE_ME` placeholders with your secure Redis password.
3. **Initialize the Swarm cluster:**
   ```bash
   docker swarm init --advertise-addr <MANAGER_IP>
   ```
4. **Deploy the stack:**
   ```bash
   docker stack deploy -c docker-stack.yml path
   ```


#### Phase B: Worker Node Setup
1. Connect to the secondary server via SSH.
2. Execute the `docker swarm join` command retrieved in Phase A.
3. The Manager node will automatically deploy and synchronize the `worker` service to the new node.

---

## Configuration Reference

Parameters are defined via environment variables in the YAML configuration files or a `.env` file.

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ROLE` | `solo` | Node behavior: `solo`, `master`, or `worker`. |
| `REDIS_URL` | - | Redis connection string (e.g., `redis://127.0.0.1:6379`). |
| `REDIS_PASSWORD`| - | Required password for Redis authentication. |
| `PATH_DNS` | `1` | Upstream DNS provider selection (1-6). |
| `ROUTE_ALL` | `n` | If `y`, proxies ALL traffic except `exclude-hosts`. |
| `BLOCK_ADS` | `y` | Enable/Disable Adblock filtering (RPZ). |
| `FILTER_CASINO` | `y` | Aggressively strip gambling domains from all lists. |
| `ENABLE_IPV6` | `y` | Enable dual-stack IPv6 support (DNS and routing). |
| `PUBLIC_DNS` | `n` | Allow DNS to listen on external IP (Security risk). |
| `AGGREGATE_COUNT`| `500` | Target limit for the number of IP prefixes in NFTables. |
| `IP` | `10` | Base IP prefix for the local gateway (default: 10.77.77.77). |
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

## List Management

All configuration files are located in the `./lists` directory on the host machine.

### Supported Formats
- **Plain Domains:** `example.com`
- **Adblock (DNS-level):** `||example.com^` (Contextual options like `$third-party` are ignored; only the domain is extracted).
- **RPZ:** `example.com CNAME .`
- **IP/CIDR:** `1.2.3.4` or `192.168.0.0/24`

### Automatic Updates
The system performs a full synchronization daily at 03:00. Manual trigger:
```bash
docker exec path /root/path/process.py
```

---

## Security and Operational Requirements

- **Privileges:** Requires `privileged: true` and `network_mode: host` to manage NFTables and apply `sysctl` optimizations (BBR, TCP tuning).
- **Monitoring & Logs:** 
    - **Unified Logs:** `docker logs -f path`
    - **IPv4 Routes:** `docker exec path nft list map inet path v4_map`
    - **IPv6 Routes:** `docker exec path nft list map inet path v6_map`
    - **Knot Stats:** `docker exec path sh -c "echo 'worker.stats()' | socat -T 1 - unix-connect:/run/knot-resolver/control/1"`
