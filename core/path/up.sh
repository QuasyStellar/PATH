#!/bin/bash
set -e
cd "$(dirname "${BASH_SOURCE[0]}")"
./down.sh
if [[ -f ".env" ]]; then
    set -a
    . ./.env
    set +a
fi
ip addr add "${IP:-10}.77.77.77/32" dev lo 2>/dev/null || true
ip addr add "${IP:-10}.88.88.88/32" dev lo 2>/dev/null || true
M4="${FAKE_NETMASK_V4:-15}"; M6="${FAKE_NETMASK_V6:-111}"
F4="${FAKE_IP:-198.18}"; F6="${FAKE_IP6:-fd00:18::}"
NFT_TMP="$(mktemp /tmp/path.XXXXXX.nft)"
cleanup_tmp() {
    rm -f "$NFT_TMP"
}
trap cleanup_tmp EXIT

ALLOWED_V4="{ 127.0.0.0/8, 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 100.64.0.0/10 }"
ALLOWED_V6="{ ::1/128, fe80::/10, fd00::/8 }"

cat <<EOF > "$NFT_TMP"
table inet path {
    map v4_map { type ipv4_addr : ipv4_addr; }
    $( [[ "$ENABLE_IPV6" == "y" ]] && echo "map v6_map { type ipv6_addr : ipv6_addr; }" )
    set deny_v4 { type ipv4_addr; flags interval; }
    $( [[ "$ENABLE_IPV6" == "y" ]] && echo "set deny_v6 { type ipv6_addr; flags interval; }" )

    chain input {
        type filter hook input priority 0; policy accept;
        iifname "lo" accept
        ct state established,related accept
        ip saddr @deny_v4 drop
        $( [[ "$ENABLE_IPV6" == "y" ]] && echo "ip6 saddr @deny_v6 drop" )
        ip saddr $ALLOWED_V4 tcp dport 6379 accept
        tcp dport 6379 drop
        ip6 saddr $ALLOWED_V6 tcp dport 6379 accept
        tcp dport 6379 drop
        $( [[ "$PUBLIC_DNS" == "n" ]] && echo "ip saddr != $ALLOWED_V4 udp dport 53 drop" )
        $( [[ "$PUBLIC_DNS" == "n" ]] && echo "ip saddr != $ALLOWED_V4 tcp dport 53 drop" )
        $( [[ "$PUBLIC_DNS" == "n" ]] && echo "ip saddr != $ALLOWED_V4 tcp dport ${DOH_PORT:-443} drop" )
        $( [[ "$ENABLE_IPV6" == "y" && "$PUBLIC_DNS" == "n" ]] && echo "ip6 saddr != $ALLOWED_V6 udp dport 53 drop" )
        $( [[ "$ENABLE_IPV6" == "y" && "$PUBLIC_DNS" == "n" ]] && echo "ip6 saddr != $ALLOWED_V6 tcp dport 53 drop" )
        $( [[ "$ENABLE_IPV6" == "y" && "$PUBLIC_DNS" == "n" ]] && echo "ip6 saddr != $ALLOWED_V6 tcp dport ${DOH_PORT:-443} drop" )
        udp dport 53 meter dns_udp_meter { ip saddr limit rate ${DNS_RATE_LIMIT:-300}/second } accept
        tcp dport 53 ct state new meter dns_tcp_new_meter { ip saddr limit rate 20/second } accept
        tcp dport 53 meter dns_tcp_meter { ip saddr limit rate ${DNS_RATE_LIMIT:-300}/second } accept
        tcp dport ${DOH_PORT:-443} ct state new meter doh_tcp_new_meter { ip saddr limit rate 20/second } accept
        tcp dport ${DOH_PORT:-443} meter doh_tcp_meter { ip saddr limit rate ${DNS_RATE_LIMIT:-300}/second } accept
        udp dport 53 drop
        tcp dport 53 drop
        $( [[ "$DOH_ENABLE" == "y" && "$PUBLIC_DNS" == "n" ]] && echo "tcp dport ${DOH_PORT:-443} drop" )
    }
    chain forward {
        type filter hook forward priority 0; policy accept;
        ip saddr @deny_v4 drop
        $( [[ "$ENABLE_IPV6" == "y" ]] && echo "ip6 saddr @deny_v6 drop" )
    }
    chain postrouting {
        type nat hook postrouting priority 100; policy accept;
        masquerade
    }
    chain filter_postrouting {
        type filter hook postrouting priority 300; policy accept;
        tcp flags syn tcp option maxseg size set rt mtu
    }
    chain raw_prerouting {
        type filter hook prerouting priority -300; policy accept;
        iifname "lo" notrack
    }
    chain raw_output {
        type filter hook output priority -300; policy accept;
        oifname "lo" notrack
    }
    chain nat_prerouting {
        type nat hook prerouting priority -100; policy accept;
        ip daddr ${F4}.0.0/${M4} dnat ip to ip daddr map @v4_map
        $( [[ "$ENABLE_IPV6" == "y" ]] && echo "ip6 daddr ${F6}/${M6} dnat ip6 to ip6 daddr map @v6_map" )
    }
    chain nat_output {
        type nat hook output priority -100; policy accept;
        ip daddr ${F4}.0.0/${M4} dnat ip to ip daddr map @v4_map
        $( [[ "$ENABLE_IPV6" == "y" ]] && echo "ip6 daddr ${F6}/${M6} dnat ip6 to ip6 daddr map @v6_map" )
    }
}
EOF
D4="result/deny-ips.txt"; D6="result/deny-ips-v6.txt"
if [[ -f "$D4" ]]; then
    IPS=$(tr '\n' ',' < "$D4" | sed 's/,$//')
    if [[ -n "$IPS" ]]; then
        echo "add element inet path deny_v4 { $IPS }" >> "$NFT_TMP"
    fi
fi
if [[ "$ENABLE_IPV6" == "y" && -f "$D6" ]]; then
    IPS=$(tr '\n' ',' < "$D6" | sed 's/,$//')
    if [[ -n "$IPS" ]]; then
        echo "add element inet path deny_v6 { $IPS }" >> "$NFT_TMP"
    fi
fi

nft -f "$NFT_TMP"

for dev in /sys/class/net/*; do
    bn="$(basename "$dev")"
    [[ "$bn" == "lo" || "$bn" == *docker* || "$bn" == *veth* ]] && continue
    [ -e "$dev/device" ] || continue
    ethtool -K "$bn" tso off gso off gro off rx-udp-gro-forwarding off 2>/dev/null || true
    ip link set "$bn" txqueuelen 1000 2>/dev/null || true
    CPU_MASK=$(printf '%x' $(( (1 << $(nproc)) - 1 )))
    for rps in "$dev"/queues/rx-*/rps_cpus; do
        [ -e "$rps" ] && echo "$CPU_MASK" > "$rps" 2>/dev/null || true
    done
done

exit 0
