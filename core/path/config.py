import os
import socket
from pathlib import Path

WORKDIR = Path(__file__).parent.absolute()
ENV_FILE = WORKDIR / ".env"


class Config:
    def __init__(self):
        self._env = {}
        self._load_env_file()
        self._load_os_environ()
        self._my_id = socket.gethostname()

    def _unquote_env_value(self, val):
        if val is None:
            return None
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] == "'":
            inner = val[1:-1]
            return inner.replace("'\\''", "'")
        if len(val) >= 2 and val[0] == val[-1] == '"':
            inner = val[1:-1]
            return inner.replace('\\"', '"').replace("\\\\", "\\")
        return val

    def _load_env_file(self):
        if ENV_FILE.exists():
            with open(ENV_FILE, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and "=" in line and not line.startswith("#"):
                        k, v = line.split("=", 1)
                        self._env[k.strip()] = self._unquote_env_value(v.strip())

    def _load_os_environ(self):
        for k, v in os.environ.items():
            self._env[k] = v

    def get(self, key, default=None, cast=None):
        val = self._env.get(key, default)
        if val is None:
            return None
        if cast is bool:
            return str(val).lower() in ("y", "yes", "true", "1")
        if cast is int:
            try:
                return int(val)
            except (ValueError, TypeError):
                return default
        return val

    @property
    def my_id(self):
        return self._my_id

    @property
    def debug(self):
        return self.get("DEBUG", "n", cast=bool)

    @property
    def node_role(self):
        return self.get("NODE_ROLE", "solo").lower()

    @property
    def redis_url(self):
        return self.get("REDIS_URL")

    @property
    def redis_password(self):
        return self.get("REDIS_PASSWORD")

    @property
    def path_dns(self):
        return self.get("PATH_DNS", "1")

    @property
    def route_all(self):
        return self.get("ROUTE_ALL", "n", cast=bool)

    @property
    def block_ads(self):
        return self.get("BLOCK_ADS", "y", cast=bool)

    @property
    def filter_casino(self):
        return self.get("FILTER_CASINO", "y", cast=bool)

    @property
    def enable_ipv6(self):
        return self.get("ENABLE_IPV6", "y", cast=bool)

    @property
    def ipv6_proxy_only(self):
        return self.get("IPV6_PROXY_ONLY", "n", cast=bool)

    @property
    def public_dns(self):
        return self.get("PUBLIC_DNS", "n", cast=bool)

    @property
    def aggregate_count(self):
        return self.get("AGGREGATE_COUNT", 500, cast=int)

    @property
    def ip_prefix(self):
        return self.get("IP", "10")

    @property
    def external_ip(self):
        return self.get("EXTERNAL_IP")

    @property
    def fake_ip(self):
        return self.get("FAKE_IP", "198.18")

    @property
    def fake_netmask_v4(self):
        return self.get("FAKE_NETMASK_V4", "15", cast=int)

    @property
    def fake_ip6(self):
        return self.get("FAKE_IP6", "fd00:18::")

    @property
    def fake_netmask_v6(self):
        return self.get("FAKE_NETMASK_V6", "111", cast=int)

    @property
    def dns_rate_limit(self):
        return self.get("DNS_RATE_LIMIT", 300, cast=int)

    @property
    def proxy_addr(self):
        return self.get("PROXY_ADDR", "127.0.0.3")

    @property
    def proxy_port(self):
        return self.get("PROXY_PORT", 53, cast=int)


config = Config()
