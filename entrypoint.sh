#!/bin/bash
set -e

log() { printf "[$(date +'%H:%M:%S')] %-9s %-12s | %s\n" "[INFO]" "BOOTSTRAP" "$1"; }

mkdir -p /root/path/lists/manual /root/path/lists/sources /root/path/result /root/path/download/temp

for f in /usr/src/path/defaults/lists/sources/*.txt; do
    dst="/root/path/lists/sources/$(basename "$f")"
    if [ ! -s "$dst" ]; then cp -f "$f" "$dst"; fi
done

for f in /usr/src/path/defaults/lists/manual/*.txt; do
    dst="/root/path/lists/manual/$(basename "$f")"
    if [ ! -s "$dst" ]; then cp -f "$f" "$dst"; fi
done

IFACE=$(ip -4 route show default | awk '{print $5}' | head -n 1)
[[ -z "$IFACE" ]] && IFACE=$(ip -4 route show | grep default | awk '{print $5}' | head -n 1)
AUTO_EXT_IP=$(ip -4 addr show dev "$IFACE" | awk '/inet / {print $2}' | cut -d/ -f1 | head -n 1)

export PATH_DNS=${PATH_DNS:-1}
export BLOCK_ADS=${BLOCK_ADS:-y}
export ENABLE_IPV6=${ENABLE_IPV6:-y}
export PUBLIC_DNS=${PUBLIC_DNS:-n}
export FILTER_CASINO=${FILTER_CASINO:-y}
export ROUTE_ALL=${ROUTE_ALL:-n}
export AGGREGATE_COUNT=${AGGREGATE_COUNT:-500}
export IP=${IP:-10}
export FAKE_IP=${FAKE_IP:-198.18}
export FAKE_IP6=${FAKE_IP6:-fd00:18::}
export EXTERNAL_IP=${EXTERNAL_IP:-$AUTO_EXT_IP}
export FAKE_NETMASK_V4=${FAKE_NETMASK_V4:-15}
export FAKE_NETMASK_V6=${FAKE_NETMASK_V6:-111}
export DNS_RATE_LIMIT=${DNS_RATE_LIMIT:-300}
export PROXY_ADDR=${PROXY_ADDR:-127.0.0.3}
export PROXY_PORT=${PROXY_PORT:-53}

export DOH_ENABLE=${DOH_ENABLE:-n}
export DOH_PORT=${DOH_PORT:-443}
export DOH_CERT=${DOH_CERT:-}
export DOH_KEY=${DOH_KEY:-}
export DOH_DOMAIN=${DOH_DOMAIN:-}
export DOH_GENERATE_CERT=${DOH_GENERATE_CERT:-n}

if [[ "$DOH_ENABLE" == "y" ]]; then
    SSL_DIR="/etc/knot-resolver/ssl"
    mkdir -p "$SSL_DIR"
    if [[ "$DOH_GENERATE_CERT" == "y" && -n "$DOH_DOMAIN" ]]; then
        if [ ! -f "/etc/letsencrypt/live/$DOH_DOMAIN/fullchain.pem" ]; then
            log "Generating SSL certificate for $DOH_DOMAIN via Certbot..."
            certbot certonly --standalone -d "$DOH_DOMAIN" --non-interactive --agree-tos --register-unsafely-without-email
        fi
        chmod -R 755 /etc/letsencrypt/archive/ /etc/letsencrypt/live/
        DOH_CERT="/etc/letsencrypt/live/$DOH_DOMAIN/fullchain.pem"
        DOH_KEY="/etc/letsencrypt/live/$DOH_DOMAIN/privkey.pem"
    fi
    if [[ -n "$DOH_CERT" && -f "$DOH_CERT" && -n "$DOH_KEY" && -f "$DOH_KEY" ]]; then
        cp -fL "$DOH_CERT" "$SSL_DIR/server.crt"
        cp -fL "$DOH_KEY" "$SSL_DIR/server.key"
        DOH_CERT="$SSL_DIR/server.crt"
        DOH_KEY="$SSL_DIR/server.key"
    elif [[ -z "$DOH_CERT" || -z "$DOH_KEY" ]]; then
        if [ ! -f "$SSL_DIR/server.crt" ]; then
            log "Generating self-signed fallback certificate..."
            openssl req -x509 -newkey rsa:2048 -keyout "$SSL_DIR/server.key" -out "$SSL_DIR/server.crt" -days 3650 -nodes -subj "/CN=doh-selfsigned"
        fi
        DOH_CERT="$SSL_DIR/server.crt"
        DOH_KEY="$SSL_DIR/server.key"
    fi
    export DOH_CERT DOH_KEY
fi

quote_env() {
    printf "'%s'" "$(printf "%s" "$1" | sed "s/'/'\\\\''/g")"
}

NODE_ROLE=${NODE_ROLE:-solo}
cat <<EOF > /root/path/.env
NODE_ROLE=$(quote_env "$NODE_ROLE")
REDIS_URL=$(quote_env "$REDIS_URL")
REDIS_PASSWORD=$(quote_env "$REDIS_PASSWORD")
PATH_DNS=$(quote_env "$PATH_DNS")
ROUTE_ALL=$(quote_env "$ROUTE_ALL")
BLOCK_ADS=$(quote_env "$BLOCK_ADS")
FILTER_CASINO=$(quote_env "$FILTER_CASINO")
ENABLE_IPV6=$(quote_env "$ENABLE_IPV6")
PUBLIC_DNS=$(quote_env "$PUBLIC_DNS")
AGGREGATE_COUNT=$(quote_env "$AGGREGATE_COUNT")
IP=$(quote_env "$IP")
EXTERNAL_IP=$(quote_env "$EXTERNAL_IP")
FAKE_IP=$(quote_env "$FAKE_IP")
FAKE_NETMASK_V4=$(quote_env "$FAKE_NETMASK_V4")
FAKE_IP6=$(quote_env "$FAKE_IP6")
FAKE_NETMASK_V6=$(quote_env "$FAKE_NETMASK_V6")
DOH_ENABLE=$(quote_env "$DOH_ENABLE")
DOH_PORT=$(quote_env "$DOH_PORT")
DOH_DOMAIN=$(quote_env "$DOH_DOMAIN")
DOH_GENERATE_CERT=$(quote_env "$DOH_GENERATE_CERT")
DOH_CERT=$(quote_env "$DOH_CERT")
DOH_KEY=$(quote_env "$DOH_KEY")
EOF
chmod 600 /root/path/.env

cleanup() {
    printf "\n[$(date +'%H:%M:%S')] %-9s %-12s | %s\n" "[INFO]" "SYSTEM" "Container stopping, cleaning up..."
    /root/path/down.sh 2>/dev/null || true
    exit 0
}
trap cleanup SIGTERM SIGINT

log "PATH initializing as ${NODE_ROLE^^}..."
ulimit -n 524288
log "PATH DNS: ${IP:-10}.77.77.77"
log "Full DNS: ${IP:-10}.88.88.88"
sysctl -p /etc/sysctl.d/99-path.conf >/dev/null || true

if [[ "$NODE_ROLE" == "worker" ]]; then
    sed -i '/\[program:cron\]/,$d' /etc/supervisor/conf.d/supervisord.conf
    cat <<EOF >> /etc/supervisor/conf.d/supervisord.conf

[program:sync-listener]
command=/root/path/sync_listener.py
autostart=true
autorestart=true
stdout_logfile=/dev/stdout
stdout_logfile_maxbytes=0
stderr_logfile=/dev/stderr
stderr_logfile_maxbytes=0
EOF
fi

log "Starting PATH Engine..."
if ! /root/path/process.py; then
    log "CRITICAL: PATH Engine failed to perform initial sync. Exiting." "ERROR"
    exit 1
fi

log "Applying network routing rules..."
/root/path/up.sh

if [[ "$NODE_ROLE" != "worker" ]]; then
    echo "0 3 * * * root . /root/path/.env; /root/path/process.py > /proc/1/fd/1 2>&1" > /etc/cron.d/path-sync
    chmod 0644 /etc/cron.d/path-sync
fi

log "Starting PATH services via Supervisor..."

DATA_COUNT=$(find /root/path/lists -name "*.txt" -exec grep -v '^#' {} + | grep -v '^[[:space:]]*$' | wc -l || echo 0)

if [ "${DATA_COUNT:-0}" -eq 0 ]; then
    echo -e "\n\e[1;33m[WARNING] YOUR PROXY LISTS ARE EMPTY!\e[0m"
    echo -e "Add your sources to: \e[1;34m./lists/sources/\e[0m"
    echo -e "Add custom domains to: \e[1;34m./lists/manual/\e[0m"
    echo -e "Then run: \e[1;32mdocker exec path /root/path/process.py\e[0m\n"
fi

/usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf &
SUPERVISOR_PID=$!
wait $SUPERVISOR_PID
cleanup
