# PATH (Policy-Aware Traffic Handler)

PATH is an industrial-grade, asynchronous DNS-based traffic routing and filtering system. It leverages Python 3.12 (asyncio), Knot Resolver, and nftables to implement transparent Fake-IP routing at scale.

It redirects traffic for selected domains through a gateway without distributing large routing tables to clients.

---

## DNS Service Modes

PATH provides two independent DNS service addresses for different operational needs:

- **PATH DNS (`10.77.77.77`)**: Adaptive traffic handling mode. It resolves specific or selected domains to Fake-IP addresses for routing through the gateway, while returning real IPs for everything else. This is the default recommended choice for policy-based traffic management.
- **Full DNS (`10.88.88.88`)**: Clean recursive mode. It returns real IPs for **all** domains but still applies filtering for ads, gambling resources, and privacy enhancements (e.g., .arpa junk filtering).

---

## Configuration Reference

All parameters are defined via environment variables in the YAML configuration files.

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ROLE` | `solo` | Node behavior: `solo`, `master`, or `worker`. |
| `REDIS_URL` | - | Redis connection string (e.g., `redis://127.0.0.1:6379`). |
| `REDIS_PASSWORD`| - | Required password for Redis authentication. |
| `PATH_DNS` | `1` | Upstream DNS selection (1-6) or custom comma-separated IPs. |
| `ROUTE_ALL` | `n` | If `y`, handles ALL traffic as specific (except `exclude-hosts`). |
| `BLOCK_ADS` | `y` | Enable/Disable ad-filtering zones (RPZ). |
| `FILTER_CASINO` | `y` | Aggressively strip gambling domains from text and adblock lists. |
| `DNS_RATE_LIMIT` | `300` | Max UDP DNS queries per second per source IP. |
| `ENABLE_IPV6` | `y` | Enable dual-stack IPv6 support (DNS and routing). |
| `PUBLIC_DNS` | `n` | Allow DNS to listen on external IP (PATH DNS only). |
| `AGGREGATE_COUNT`| `500` | Target limit for the number of IP prefixes in nftables. |
| `IP` | `10` | Base IPv4 prefix for local gateways (e.g., `10.77.77.77`). |
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

### PATH_DNS Upstream Sets

| `PATH_DNS` | Description | Upstream IPs |
|-----------|-------------|--------------|
| `1` | Cloudflare+Quad9 + MSK-IX+NSDI [*] | `1.1.1.1`, `1.0.0.1`, `9.9.9.10`, `149.112.112.10`, `62.76.76.62`, `62.76.62.76`, `195.208.4.1`, `195.208.5.1` |
| `2` | Cloudflare+Quad9 + SkyDNS [*][1] | `1.1.1.1`, `1.0.0.1`, `9.9.9.10`, `149.112.112.10`, `193.58.251.251` |
| `3` | Cloudflare+Quad9 | `1.1.1.1`, `1.0.0.1`, `9.9.9.10`, `149.112.112.10` |
| `4` | Comss | `83.220.169.155`, `212.109.195.93`, `195.133.25.16` |
| `5` | XBox | `176.99.11.77`, `80.78.247.254`, `31.192.108.180` |
| `6` | Malware | `84.21.189.133`, `193.23.209.189` |

| Note | Meaning |
|------|---------|
| [*] | Regional resolvers with automated global fallbacks for reliability. |
| [1] | Requires a SkyDNS account and adding this server IP to their dashboard. |

---

## Deployment Modes

### Method 1: Standalone (solo)
Suitable for a single server. DNS zones and mappings are managed locally.

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
Distributed setup for multiple nodes with **Automated Failover**. If the active Master node becomes unresponsive (heartbeat timeout > 15m), any available node in the cluster will automatically take over management tasks.

1. Prepare the environment and retrieve the stack configuration:
```bash
mkdir -p /opt/path && cd /opt/path
wget https://raw.githubusercontent.com/QuasyStellar/PATH/main/docker-stack.yml
```

2. Edit `docker-stack.yml` and replace `REPLACE_ME` placeholders with your Redis password.

3. Initialize the Swarm cluster and deploy:
```bash
docker swarm init --advertise-addr <MANAGER_IP>
docker stack deploy -c docker-stack.yml path
```

---

## List Management

All configuration files are located in the `./lists` directory.

Supported formats:
- **Plain domains**: `example.com`
- **Adblock syntax**: `||example.com^` (domain extraction only)
- **RPZ zones**: `example.com CNAME .`
- **IP/CIDR**: `1.2.3.4` or `192.168.0.0/24`

To trigger manual list processing:
```bash
docker exec path /root/path/process.py
```

---

## Security & Network Access

### Redis Security
By default, the Redis service configuration (if uncommented) is set to bind to `127.0.0.1:6379`. This prevents external access from the internet.

### DNS & DoH Access
Access is managed by `nftables` via `up.sh`:
- **Private Mode (`PUBLIC_DNS=n`)**: By default, access is restricted to local networks and the `100.64.0.0/10` range (Netbird/Tailscale).
- **Public Mode (`PUBLIC_DNS=y`)**: Ports 53 and 443 are open to all interfaces but protected by rate-limiting to prevent DDoS.

---

## Diagnostics

### Check nftables maps
```bash
nft list map inet path v4_map
nft list map inet path v6_map
```

### Knot Resolver stats
```bash
# PATH DNS (Instance 1)
docker exec path sh -c "echo 'worker.stats()' | socat -T 1 - unix-connect:/run/knot-resolver/control/1"
# Full DNS (Instance 2)
docker exec path sh -c "echo 'worker.stats()' | socat -T 1 - unix-connect:/run/knot-resolver/control/2"
```

### Logs
```bash
docker logs -f path
```
