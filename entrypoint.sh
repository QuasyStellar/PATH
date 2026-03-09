#!/bin/bash
set -e

log() { echo "[$(date +'%H:%M:%S')] [INFO] BOOTSTRAP | $1"; }

mkdir -p /root/path/lists/manual /root/path/lists/sources /root/path/result /root/path/download /root/path/temp

for f in /usr/src/path/defaults/lists/sources/*.txt; do
    dst="/root/path/lists/sources/$(basename "$f")"
    if [ ! -s "$dst" ]; then
        cp -f "$f" "$dst"
    fi
done

for f in /usr/src/path/defaults/lists/manual/*.txt; do
    dst="/root/path/lists/manual/$(basename "$f")"
    if [ ! -s "$dst" ]; then
        cp -f "$f" "$dst"
    fi
done

cp -f /usr/src/path/defaults/*.py /root/path/
cp -f /usr/src/path/defaults/*.sh /root/path/
chmod +x /root/path/*.sh /root/path/*.py

IFACE=$(ip route get 1.2.3.4 2>/dev/null | awk '{print $5; exit}')
[ -z "$IFACE" ] && IFACE=$(ip -4 route show | grep default | awk '{print $5}' | head -n 1)
AUTO_EXT_IP=$(ip -4 addr show "$IFACE" | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | head -n 1)
EXTERNAL_IP=${EXTERNAL_IP:-$AUTO_EXT_IP}
export EXTERNAL_IP

cat <<EOF > /root/path/.env
PATH_DNS=${PATH_DNS:-1}
BLOCK_ADS=${BLOCK_ADS:-y}
ENABLE_IPV6=${ENABLE_IPV6:-y}
PUBLIC_DNS=${PUBLIC_DNS:-n}
FILTER_CASINO=${FILTER_CASINO:-y}
ROUTE_ALL=${ROUTE_ALL:-n}
AGGREGATE_COUNT=${AGGREGATE_COUNT:-500}
IP=${IP:-10}
FAKE_IP=${FAKE_IP:-198.18}
FAKE_IP6=${FAKE_IP6:-fd00:18::}
EXTERNAL_IP=$EXTERNAL_IP
FAKE_NETMASK_V4=${FAKE_NETMASK_V4:-15}
FAKE_NETMASK_V6=${FAKE_NETMASK_V6:-111}
EOF

cleanup() {
    echo -e "\n[$(date +'%H:%M:%S')] [INFO] SYSTEM | Container stopping, cleaning up..."
    /root/path/down.sh 2>/dev/null || true
    exit 0
}
trap cleanup SIGTERM SIGINT

log "PATH initializing..."
sysctl -p /etc/sysctl.d/99-path.conf || true
rm -f /root/path/result/*.rpz

DATA_COUNT=$(grep -v '^#' /root/path/lists/sources/*.txt /root/path/lists/manual/*.txt 2>/dev/null | grep -v '^[[:space:]]*$' | wc -l)

log "Starting PATH Engine..."
/root/path/process.py

log "Applying network routing rules..."
/root/path/up.sh

echo "0 3 * * * /root/path/process.py" | crontab -

log "Starting PATH services via Supervisor..."

if [ "$DATA_COUNT" -eq 0 ]; then
    echo -e "\n\e[1;33m[WARNING] YOUR PROXY LISTS ARE EMPTY!\e[0m"
    echo -e "Add your sources to: \e[1;34m./lists/sources/\e[0m"
    echo -e "Add custom domains to: \e[1;34m./lists/manual/\e[0m"
    echo -e "Then run: \e[1;32mdocker exec path /root/path/process.py\e[0m\n"
fi

exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf &
SUPERVISOR_PID=$!
wait $SUPERVISOR_PID
cleanup
