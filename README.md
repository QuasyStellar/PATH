# PATH (Policy-Aware Traffic Handler)

PATH is a high-performance, asynchronous DNS-based traffic routing and filtering system built with a modern network stack, featuring Python 3.12 Async, Knot Resolver, and NFTables.

## Key Features

- **Asynchronous DNS Proxy:** Built with Python 3.12 `asyncio`, capable of handling thousands of concurrent requests without blocking.
- **Kernel-Level Routing (NFTables):** Utilizes NFTables Maps for O(1) routing lookups, drastically reducing CPU overhead compared to legacy solutions.
- **Suffix Trie Domain Optimization:** Mathematically optimal domain collapsing algorithm that automatically merges subdomains into wildcard records.
- **Adblock Integration:** Built-in DNS-level protection against ads, trackers, and malware via RPZ (Response Policy Zones).
- **Dual-Stack Support:** Native support for both IPv4 and IPv6 protocols for resolution and routing.
- **Casino Clean:** Advanced regex-based filtering system to strip gambling and betting domains from your proxy lists.
- **Production-Ready Docker:** Monolithic "Core" container with automated environment detection and service management via Supervisor.

---

## Installation

### Docker Deployment (Recommended)

The fastest way to deploy PATH. All dependencies and configurations are pre-packaged.

**Quick Start:**
```bash
mkdir -p /root/path && cd /root/path
wget -O docker-compose.yml https://raw.githubusercontent.com/QuasyStellar/PATH/main/docker-compose.yml
docker compose up -d
```

### Build Docker from Source

To customize the code or build your own image:
1. Clone the repository:
   ```bash
   git clone https://github.com/QuasyStellar/PATH.git && cd PATH
   ```
2. Build and run:
   ```bash
   docker compose up -d --build
   ```

---

## Configuration

System parameters can be managed via environment variables in `docker-compose.yml` or the `/root/path/.env` file.

| Variable | Values | Description |
|----------|---------|-------------|
| `PATH_DNS` | `1-6` | Upstream DNS choice: 1—Cloudflare+Quad9+SkyDNS, 2—SkyDNS, 3—Cloudflare+Quad9, 4—Comss, 5—XBox, 6—Malw. |
| `ROUTE_ALL` | `y/n` | Enable "Proxy All" mode (proxies every domain except those in manual/exclude-hosts). |
| `BLOCK_ADS` | `y/n` | Enable DNS-level ad and tracker blocking (AdGuard + OISD sources). |
| `FILTER_CASINO`| `y/n` | Filter out gambling and betting domains during list generation. |
| `ENABLE_IPV6` | `y/n` | Enable dual-stack IPv6 support. |
| `PUBLIC_DNS` | `y/n` | Allow the DNS server to listen on the external IP (Public DNS mode). |
| `AGGREGATE_COUNT`| `int` | Maximum number of aggregated IP prefixes in the routing table (default: 500). |
| `IP` | `10/172`| Base IP prefix for the local gateway (10.77.77.77 or 172.77.77.77). |
| `FAKE_IP` | `string`| IPv4 prefix for Fake-IP mapping (default: 198.18). |
| `FAKE_NETMASK_V4`| `int` | CIDR mask for IPv4 Fake-IP range (default: 15). |
| `FAKE_IP6` | `string`| IPv6 prefix for Fake-IP mapping (default: fd00:18::). |
| `FAKE_NETMASK_V6`| `int` | CIDR mask for IPv6 Fake-IP range (default: 111). |
| `EXTERNAL_IP` | `auto/IP`| The external IP address of the server (detected automatically if not set). |

### Docker Requirements

To manage the host's networking stack, the container requires:
- `network_mode: host` (Essential for NFTables and DNAT).
- `privileged: true` (Required for applying `sysctl` kernel optimizations).
- **Volumes:**
    - `./lists`: User-defined domain/IP lists (populated with templates on first run).
    - `./result`: Output directory for generated routing and RPZ files.

---

## Resource Management

Configuration is split into **Manual** lists and **Automated** sources. Templates with instructions are created in the `./lists` directory after the first run.

### 1. Manual Lists (`lists/manual/`)
Add domains or IPs directly to these files:
- `include-hosts.txt`: Domains to be proxied.
- `exclude-hosts.txt`: Direct resolution bypass (always direct).
- `include-ips.txt`: Specific IP addresses or CIDR ranges to proxy.

### 2. External Sources (`lists/sources/`)
Files in this directory should contain **URLs** pointing to raw text lists. The system will automatically download and merge them.

### Applying Changes
Lists are updated automatically once a day. For manual synchronization:
- **Docker:** `docker exec path /root/path/process.py`
