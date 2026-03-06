#!/bin/bash
export LC_ALL=C
[[ "$EUID" -ne 0 ]] && echo 'Error: root required!' && exit 3

if [[ "$0" == "bash" || "$0" == "/bin/bash" || "$0" == "/dev/fd/"* ]]; then
    echo "Running from pipe, cloning repository..."
    apt-get update && apt-get install -y git
    rm -rf /tmp/path-installer
    git clone --depth 1 https://github.com/QuasyStellar/PATH.git /tmp/path-installer
    REPO_DIR="/tmp/path-installer"
else
    REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi

if [[ ! -d "$REPO_DIR/core" ]]; then
    echo "Error: 'core' directory not found in $REPO_DIR"
    exit 1
fi

cd /root
echo -e '\e[1;32mInstalling PATH (Policy-Aware Traffic Handler)...\e[0m\n'

echo 'Choose DNS resolvers for PATH:'
echo '    1) Cloudflare+Quad9+SkyDNS (Recommended)'
echo '    2) SkyDNS (Expert)'
echo '    3) Cloudflare+Quad9'
echo '    4) Comss'
echo '    5) XBox'
echo '    6) Malw'
until [[ "$PATH_DNS" =~ ^[1-6]$ ]]; do read -rp 'Choice [1-6]: ' -e -i 1 PATH_DNS; done
echo

until [[ "$ROUTE_ALL" =~ (y|n) ]]; do read -rp 'Proxy all traffic for domains via PATH, excluding domains from lists/manual/exclude-hosts.txt? [y/n]: ' -e -i n ROUTE_ALL; done
echo

until [[ "$BLOCK_ADS" =~ (y|n) ]]; do read -rp 'Block ads, trackers, malware and phishing websites based on AdGuard and OISD rules? [y/n]: ' -e -i y BLOCK_ADS; done
echo

until [[ "$FILTER_CASINO" =~ (y|n) ]]; do read -rp 'Exclude gambling and betting domains from PATH lists? [y/n]: ' -e -i y FILTER_CASINO; done
echo

until [[ "$ENABLE_IPV6" =~ (y|n) ]]; do read -rp 'Enable IPv6 support? [y/n]: ' -e -i y ENABLE_IPV6; done
echo

until [[ "$PUBLIC_DNS" =~ (y|n) ]]; do read -rp 'Listen for DNS requests on all network interfaces (Public DNS)? [y/n]: ' -e -i n PUBLIC_DNS; done
echo

echo 'Standard FAKE IP range:      10.30.0.0/15'
echo 'Alternative FAKE IP range:   172.30.0.0/15'
until [[ "$ALTERNATIVE_IP" =~ (y|n) ]]; do read -rp 'Use alternative FAKE IP range (172.30.x.x)? [y/n]: ' -e -i n ALTERNATIVE_IP; done
echo

[[ "$ALTERNATIVE_IP" == 'y' ]] && IP=172 || IP=10
echo "Current FAKE IP range:       $IP.30.0.0/15"
echo 'Special FAKE IP range:       198.18.0.0/15'
until [[ "$ALT_FAKE" =~ (y|n) ]]; do read -rp 'Use special FAKE IP range (198.18.x.x)? [y/n]: ' -e -i y ALT_FAKE; done
echo

until [[ "$AGG_COUNT" =~ ^[0-9]+$ ]]; do read -rp 'Maximum number of IP prefixes after aggregation (Reduces routes). 500 is default: ' -e -i 500 AGG_COUNT; done
echo

apt-get update && apt-get install -y curl gpg git knot-resolver idn socat lsb-release nftables python3-dnslib python3-aiohttp python3-idna

if [[ ! -f /etc/apt/sources.list.d/cznic-labs-knot-resolver.list ]]; then
    curl -fL https://pkg.labs.nic.cz/gpg -o /etc/apt/keyrings/cznic-labs-pkg.gpg
    echo "deb [signed-by=/etc/apt/keyrings/cznic-labs-pkg.gpg] https://pkg.labs.nic.cz/knot-resolver $(lsb_release -cs) main" > /etc/apt/sources.list.d/cznic-labs-knot-resolver.list
    apt-get update && apt-get install -y knot-resolver
fi

systemctl disable --now kresd@1 kresd@2 path path-update.timer 2>/dev/null || true

[[ "$ALT_FAKE" == 'y' ]] && FAKE_IP_VAL="198.18" || FAKE_IP_VAL="$IP.30"
IFACE="$(ip route get 1.2.3.4 | awk '{print $5; exit}')"
EXT_IP="$(ip -4 addr show "$IFACE" | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | head -n 1)"

mkdir -p /root/path/lists/manual /root/path/lists/sources /root/path/result /root/path/download /usr/lib/knot-resolver/kres_modules
cp -r --update=none "$REPO_DIR"/core/path/lists/manual/* /root/path/lists/manual/ 2>/dev/null || true
cp -r --update=none "$REPO_DIR"/core/path/lists/sources/* /root/path/lists/sources/ 2>/dev/null || true
cp -f "$REPO_DIR"/core/path/*.sh /root/path/
cp -f "$REPO_DIR"/core/path/*.py /root/path/
cp -r "$REPO_DIR"/core/sys/knot/* /etc/knot-resolver/
cp -f "$REPO_DIR"/core/sys/sysctl/*.conf /etc/sysctl.d/
cp -rf "$REPO_DIR"/core/sys/systemd/*.service /etc/systemd/system/
cp -rf "$REPO_DIR"/core/sys/systemd/*.timer /etc/systemd/system/
cp -rf "$REPO_DIR"/core/usr/lib/knot-resolver/kres_modules/* /usr/lib/knot-resolver/kres_modules/

cat <<EOF > /root/path/.env
PATH_DNS=$PATH_DNS
BLOCK_ADS=$BLOCK_ADS
ENABLE_IPV6=$ENABLE_IPV6
PUBLIC_DNS=$PUBLIC_DNS
FILTER_CASINO=$FILTER_CASINO
ROUTE_ALL=$ROUTE_ALL
AGGREGATE_COUNT=$AGG_COUNT
IP=$IP
FAKE_IP=$FAKE_IP_VAL
FAKE_IP6=fd00:18::
EXTERNAL_IP=$EXT_IP
FAKE_NETMASK_V4=15
FAKE_NETMASK_V6=111
EOF

chmod +x /root/path/*.sh /root/path/*.py
sysctl -p /etc/sysctl.d/99-path.conf
/root/path/process.py

systemctl daemon-reload
systemctl enable --now kresd@1 kresd@2 path path-update.timer
systemctl mask kres-cache-gc 2>/dev/null || true

echo -e '\n\e[1;32mPATH (Policy-Aware Traffic Handler) ready!\e[0m'

DATA_COUNT=$(grep -v '^#' /root/path/lists/sources/*.txt /root/path/lists/manual/*.txt 2>/dev/null | grep -v '^[[:space:]]*$' | wc -l)

if [ "$DATA_COUNT" -eq 0 ]; then
    echo -e '\e[1;33m[WARNING] Your proxy lists are currently empty!\e[0m'
    echo -e 'Please add your sources to: \e[1;34m/root/path/lists/sources/\e[0m'
    echo -e 'And custom domains to:   \e[1;34m/root/path/lists/manual/\e[0m'
    echo -e 'Then run: \e[1;32mpython3 /root/path/process.py\e[0m'
fi
