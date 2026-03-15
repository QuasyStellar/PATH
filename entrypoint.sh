#!/bin/bash
set -e

log() { echo "[$(date +'%H:%M:%S')] [INFO] BOOTSTRAP | $1"; }

mkdir -p /root/path/lists/manual /root/path/lists/sources /root/path/result /root/path/download/temp

for f in /usr/src/path/defaults/lists/sources/*.txt; do
    dst="/root/path/lists/sources/$(basename "$f")"
    if [ ! -s "$dst" ]; then cp -f "$f" "$dst"; fi
done

for f in /usr/src/path/defaults/lists/manual/*.txt; do
    dst="/root/path/lists/manual/$(basename "$f")"
    if [ ! -s "$dst" ]; then cp -f "$f" "$dst"; fi
done

cp -f /usr/src/path/defaults/*.py /root/path/
cp -f /usr/src/path/defaults/*.sh /root/path/
chmod +x /root/path/*.sh /root/path/*.py

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
            openssl req -x509 -newkey rsa:2048 -keyout "$SSL_DIR/server.key" -out "$SSL_DIR/server.crt" -days 3650 -nodes -subj "/CN=doh-selfsigned"
        fi
        DOH_CERT="$SSL_DIR/server.crt"
        DOH_KEY="$SSL_DIR/server.key"
    fi
    export DOH_CERT DOH_KEY
fi

NODE_ROLE=${NODE_ROLE:-solo}
cat <<EOF > /root/path/.env
NODE_ROLE=$NODE_ROLE
REDIS_URL=$REDIS_URL
REDIS_PASSWORD=$REDIS_PASSWORD
PATH_DNS=$PATH_DNS
ROUTE_ALL=$ROUTE_ALL
BLOCK_ADS=$BLOCK_ADS
FILTER_CASINO=$FILTER_CASINO
ENABLE_IPV6=$ENABLE_IPV6
PUBLIC_DNS=$PUBLIC_DNS
AGGREGATE_COUNT=$AGGREGATE_COUNT
IP=$IP
EXTERNAL_IP=$EXTERNAL_IP
FAKE_IP=$FAKE_IP
FAKE_NETMASK_V4=$FAKE_NETMASK_V4
FAKE_IP6=$FAKE_IP6
FAKE_NETMASK_V6=$FAKE_NETMASK_V6
DOH_ENABLE=$DOH_ENABLE
DOH_PORT=$DOH_PORT
DOH_DOMAIN=$DOH_DOMAIN
DOH_GENERATE_CERT=$DOH_GENERATE_CERT
DOH_CERT=$DOH_CERT
DOH_KEY=$DOH_KEY
EOF
chmod 600 /root/path/.env

cleanup() {
    /root/path/down.sh 2>/dev/null || true
    exit 0
}
trap cleanup SIGTERM SIGINT

sysctl -p /etc/sysctl.d/99-path.conf || true

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

/root/path/process.py
/root/path/up.sh

if [[ "$NODE_ROLE" != "worker" ]]; then
    echo "0 3 * * * /root/path/process.py" | crontab -
fi

/usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf &
SUPERVISOR_PID=$!
wait $SUPERVISOR_PID
cleanup
