#!/bin/bash
export LC_ALL=C
[[ "$EUID" -ne 0 ]] && echo 'Error: root required!' && exit 3

INSTALL_DIR="/opt/path"
mkdir -p "$INSTALL_DIR"

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

until [[ "$ALTERNATIVE_IP" =~ (y|n) ]]; do read -rp 'Use alternative FAKE IP range (172.30.x.x)? [y/n]: ' -e -i n ALTERNATIVE_IP; done
echo

[[ "$ALTERNATIVE_IP" == 'y' ]] && IP=172 || IP=10

until [[ "$ALT_FAKE" =~ (y|n) ]]; do read -rp 'Use special FAKE IP range (198.18.x.x)? [y/n]: ' -e -i y ALT_FAKE; done
echo

until [[ "$AGGREGATE_COUNT" =~ ^[0-9]+$ ]]; do read -rp 'Maximum number of IP prefixes after aggregation (Reduces routes). 500 is default: ' -e -i 500 AGGREGATE_COUNT; done
echo

until [[ "$DOH_ENABLE" =~ (y|n) ]]; do read -rp 'Enable DNS-over-HTTPS (DoH) listener? [y/n]: ' -e -i n DOH_ENABLE; done
echo

if [[ "$DOH_ENABLE" == "y" ]]; then
    read -rp 'Enter DoH port [Default: 443]: ' -e -i 443 DOH_PORT
    echo
    echo 'DoH SSL mode:'
    echo '    1) Use existing certificates (manual path)'
    echo '    2) Generate new with Certbot (requires domain)'
    echo '    3) No SSL (Generates self-signed fallback, suitable for Nginx)'
    until [[ "$DOH_SSL_MODE" =~ ^[1-3]$ ]]; do read -rp 'Choice [1-3]: ' -e -i 3 DOH_SSL_MODE; done
    echo

    SSL_DIR="/etc/knot-resolver/ssl"
    mkdir -p "$SSL_DIR"

    if [[ "$DOH_SSL_MODE" == "1" ]]; then
        read -rp 'Enter full path to SSL certificate (fullchain.pem): ' DOH_CERT
        read -rp 'Enter full path to SSL private key (privkey.pem): ' DOH_KEY
    elif [[ "$DOH_SSL_MODE" == "2" ]]; then
        read -rp 'Enter domain for DoH (e.g. doh.example.com): ' DOH_DOMAIN
        apt-get update && apt-get install -y certbot
        if [ ! -f "/etc/letsencrypt/live/$DOH_DOMAIN/fullchain.pem" ]; then
            certbot certonly --standalone -d "$DOH_DOMAIN" --non-interactive --agree-tos --register-unsafely-without-email --deploy-hook "chmod -R 755 /etc/letsencrypt/archive/ /etc/letsencrypt/live/ && systemctl restart kresd@1 kresd@2"
        fi
        chmod -R 755 /etc/letsencrypt/archive/ /etc/letsencrypt/live/
        DOH_CERT="/etc/letsencrypt/live/$DOH_DOMAIN/fullchain.pem"
        DOH_KEY="/etc/letsencrypt/live/$DOH_DOMAIN/privkey.pem"
    elif [[ "$DOH_SSL_MODE" == "3" ]]; then
        if [ ! -f "$SSL_DIR/server.crt" ]; then
            echo "Generating self-signed fallback for DoH..."
            openssl req -x509 -newkey rsa:2048 -keyout "$SSL_DIR/server.key" -out "$SSL_DIR/server.crt" -days 3650 -nodes -subj "/CN=path-doh"
        fi
        DOH_CERT="$SSL_DIR/server.crt"
        DOH_KEY="$SSL_DIR/server.key"
    fi

    if [[ -n "$DOH_CERT" && -f "$DOH_CERT" ]]; then
        cp -fL "$DOH_CERT" "$SSL_DIR/server.crt"
        cp -fL "$DOH_KEY" "$SSL_DIR/server.key"
        DOH_CERT="$SSL_DIR/server.crt"
        DOH_KEY="$SSL_DIR/server.key"
    fi
fi

apt-get update && apt-get install -y curl gpg git knot-resolver knot-resolver-module-http idn socat lsb-release nftables python3-dnslib python3-aiohttp python3-idna certbot openssl

if [[ ! -f /etc/apt/sources.list.d/cznic-labs-knot-resolver.list ]]; then
    curl -fL https://pkg.labs.nic.cz/gpg -o /etc/apt/keyrings/cznic-labs-pkg.gpg
    echo "deb [signed-by=/etc/apt/keyrings/cznic-labs-pkg.gpg] https://pkg.labs.nic.cz/knot-resolver $(lsb_release -cs) main" > /etc/apt/sources.list.d/cznic-labs-knot-resolver.list
    apt-get update && apt-get install -y knot-resolver knot-resolver-module-http
fi

systemctl disable --now kresd@1 kresd@2 path path-update.timer 2>/dev/null || true

[[ "$ALT_FAKE" == 'y' ]] && FAKE_IP_VAL="198.18" || FAKE_IP_VAL="$IP.30"
IFACE="$(ip route get 1.2.3.4 2>/dev/null | awk '{print $5; exit}')"
[ -z "$IFACE" ] && IFACE=$(ip -4 route show | grep default | awk '{print $5}' | head -n 1)
EXT_IP="$(ip -4 addr show "$IFACE" | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | head -n 1)"

mkdir -p "$INSTALL_DIR/lists/manual" "$INSTALL_DIR/lists/sources" "$INSTALL_DIR/result" "$INSTALL_DIR/download" /usr/lib/knot-resolver/kres_modules
cp -r --update=none "$REPO_DIR"/core/path/lists/manual/* "$INSTALL_DIR/lists/manual/" 2>/dev/null || true
cp -r --update=none "$REPO_DIR"/core/path/lists/sources/* "$INSTALL_DIR/lists/sources/" 2>/dev/null || true
cp -f "$REPO_DIR"/core/path/*.sh "$INSTALL_DIR/"
cp -f "$REPO_DIR"/core/path/*.py "$INSTALL_DIR/"
cp -r "$REPO_DIR"/core/sys/knot/* /etc/knot-resolver/
cp -f "$REPO_DIR"/core/sys/sysctl/*.conf /etc/sysctl.d/
cp -rf "$REPO_DIR"/core/sys/systemd/*.service /etc/systemd/system/
cp -rf "$REPO_DIR"/core/sys/systemd/*.timer /etc/systemd/system/
cp -rf "$REPO_DIR"/core/usr/lib/knot-resolver/kres_modules/* /usr/lib/knot-resolver/kres_modules/

sed -i "s|/root/path|$INSTALL_DIR|g" /etc/systemd/system/path.service
sed -i "s|/root/path|$INSTALL_DIR|g" /etc/systemd/system/path-update.service

cat <<EOF > "$INSTALL_DIR/.env"
PATH_DNS=$PATH_DNS
IP=$IP
FAKE_IP=$FAKE_IP_VAL
FAKE_IP6=fd00:18::
EXTERNAL_IP=$EXT_IP
AGGREGATE_COUNT=$AGGREGATE_COUNT
FAKE_NETMASK_V4=15
FAKE_NETMASK_V6=111
ROUTE_ALL=$ROUTE_ALL
BLOCK_ADS=$BLOCK_ADS
FILTER_CASINO=$FILTER_CASINO
ENABLE_IPV6=$ENABLE_IPV6
PUBLIC_DNS=$PUBLIC_DNS
DOH_ENABLE=$DOH_ENABLE
DOH_PORT=${DOH_PORT:-443}
DOH_DOMAIN=$DOH_DOMAIN
DOH_CERT=$DOH_CERT
DOH_KEY=$DOH_KEY
EOF

chmod +x "$INSTALL_DIR"/*.sh "$INSTALL_DIR"/*.py
chown -R knot:knot /etc/knot-resolver
chown -R knot:knot "$INSTALL_DIR"
chmod 755 "$INSTALL_DIR"

sysctl -p /etc/sysctl.d/99-path.conf
"$INSTALL_DIR/process.py"

mkdir -p /etc/systemd/system/kresd@.service.d/
cat <<EOF > /etc/systemd/system/kresd@.service.d/path.conf
[Service]
EnvironmentFile=$INSTALL_DIR/.env
EOF

if command -v ufw >/dev/null 2>&1 && ufw status | grep -q "Status: active"; then
    ufw allow 53/udp
    ufw allow 53/tcp
    if [[ "$DOH_ENABLE" == "y" ]]; then
        ufw allow "$DOH_PORT"/tcp
    fi
fi

systemctl daemon-reload
systemctl enable --now kresd@1 kresd@2 path path-update.timer
systemctl mask kres-cache-gc 2>/dev/null || true

echo -e '\n\e[1;32mPATH (Policy-Aware Traffic Handler) ready!\e[0m'

DATA_COUNT=$(grep -v '^#' "$INSTALL_DIR/lists/sources/"*.txt "$INSTALL_DIR/lists/manual/"*.txt 2>/dev/null | grep -v '^[[:space:]]*$' | wc -l)

if [ "$DATA_COUNT" -eq 0 ]; then
    echo -e '\e[1;33m[WARNING] Your proxy lists are currently empty!\e[0m'
    echo -e "Please add your sources to: \e[1;34m$INSTALL_DIR/lists/sources/\e[0m"
    echo -e "And custom domains to:   \e[1;34m$INSTALL_DIR/lists/manual/\e[0m"
    echo -e "Then run: \e[1;32mpython3 $INSTALL_DIR/process.py\e[0m"
fi
