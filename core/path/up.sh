#!/bin/bash
set -e
cd "$(dirname "${BASH_SOURCE[0]}")"
./down.sh
if [[ -f ".env" ]]; then
    set -a
    . ./.env
    set +a
fi
ip addr add "${IP:-10}.77.77.77/32" dev lo || true
for i in 1 2; do echo "cache.clear()" | socat - unix-connect:/run/knot-resolver/control/$i 2>/dev/null || true; done
M4="${FAKE_NETMASK_V4:-15}"; M6="${FAKE_NETMASK_V6:-111}"
F4="${FAKE_IP:-198.18}"; F6="${FAKE_IP6:-fd00:18::}"
NFT_TMP="$(mktemp /tmp/path.XXXXXX.nft)"
cleanup_tmp() {
    rm -f "$NFT_TMP"
}
trap cleanup_tmp EXIT
cat <<EOF > "$NFT_TMP"
table inet path {
    map v4_map { type ipv4_addr : ipv4_addr; flags timeout; }
    $( [[ "$ENABLE_IPV6" == "y" ]] && echo "map v6_map { type ipv6_addr : ipv6_addr; flags timeout; }" )
    chain input {
        type filter hook input priority 0; policy accept;
        iifname "lo" accept
        udp dport 53 meter dns_meter { ip saddr limit rate 50/second } accept
        udp dport 53 drop
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
        ip daddr == ${F4}.0.0/${M4} dnat ip to ip daddr map @v4_map
        $( [[ "$ENABLE_IPV6" == "y" ]] && echo "ip6 daddr == ${F6}/${M6} dnat ip6 to ip6 daddr map @v6_map" )
    }
    chain nat_output {
        type nat hook output priority -100; policy accept;
        ip daddr == ${F4}.0.0/${M4} dnat ip to ip daddr map @v4_map
        $( [[ "$ENABLE_IPV6" == "y" ]] && echo "ip6 daddr == ${F6}/${M6} dnat ip6 to ip6 daddr map @v6_map" )
    }
}
EOF
nft -f "$NFT_TMP"
exit 0
