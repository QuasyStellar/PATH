#!/bin/bash
exec 2>/dev/null
cd "$(dirname "${BASH_SOURCE[0]}")"
[[ -f ".env" ]] && export $(grep -v '^#' .env | xargs)
ip addr del "${IP:-10}.77.77.77/32" dev lo || true
nft delete table inet path
exit 0
