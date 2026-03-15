#!/bin/bash
exec 2>/dev/null
cd "$(dirname "${BASH_SOURCE[0]}")"
if [[ -f ".env" ]]; then
    set -a
    . ./.env
    set +a
fi
ip addr del "${IP:-10}.77.77.77/32" dev lo || true
nft delete table inet path
exit 0
