FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV LC_ALL=C

RUN apt-get update && apt-get install -y \
    curl gpg git idn socat lsb-release nftables \
    python3-dnslib python3-aiohttp python3-idna \
    iproute2 procps cron supervisor \
    && curl -fL https://pkg.labs.nic.cz/gpg -o /etc/apt/keyrings/cznic-labs-pkg.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/cznic-labs-pkg.gpg] https://pkg.labs.nic.cz/knot-resolver $(lsb_release -cs) main" > /etc/apt/sources.list.d/cznic-labs-knot-resolver.list \
    && apt-get update && apt-get install -y knot-resolver \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /usr/src/path/defaults /etc/knot-resolver /run/knot-resolver/control \
    && touch /etc/knot-resolver/deny.rpz /etc/knot-resolver/deny2.rpz /etc/knot-resolver/proxy.rpz \
    && chmod 777 /run/knot-resolver/control

WORKDIR /usr/src/path
COPY core/path/ ./defaults/
COPY core/sys/knot/kresd.conf /etc/knot-resolver/
COPY core/sys/sysctl/99-path.conf /etc/sysctl.d/
COPY core/usr/lib/knot-resolver/kres_modules/ /usr/lib/knot-resolver/kres_modules/

WORKDIR /root/path

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 53/udp 53/tcp

ENTRYPOINT ["/entrypoint.sh"]
