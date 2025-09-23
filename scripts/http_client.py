"""HTTP client helpers with Kerberos-aware proxy support.

This module centralizes the logic required to talk to remote services when an
enterprise proxy performs Kerberos (SPNEGO) authentication.  The default
``requests`` session is extended with a custom ``HTTPAdapter`` that injects the
``Proxy-Authorization`` header using the ``requests-kerberos`` package.  The
settings are driven by environment variables so that both local development and
the OpenShift pipeline can share the same implementation.

Environment variables honoured:

``HTTP_PROXY`` / ``HTTPS_PROXY``
    Proxy URLs (for example ``http://abproxy.example.com:8080``).

``NO_PROXY``
    Hosts that should bypass the proxy.

``PROXY_CA_FILE``
    Optional path to a ``.pem`` file that should be trusted when performing TLS
    verification against the proxy.

``PROXY_KERBEROS`` (default: ``true``)
    Toggle Kerberos authentication for the proxy.

``PROXY_KERBEROS_PRINCIPAL`` / ``PROXY_KERBEROS_SERVICE``
    Optional overrides passed to ``HTTPKerberosAuth``.

``PROXY_KERBEROS_MUTUAL``
    One of ``required``, ``optional`` or ``disabled``.  Maps directly to the
    constants provided by :mod:`requests_kerberos`.

``PROXY_HEADERS``
    Extra headers to include when talking to the proxy.  Expected format is a
    semi-colon separated list (e.g. ``Header1: value; Header2: value``).

``REQUESTS_CA_BUNDLE`` / ``CURL_CA_BUNDLE``
    Standard variables honoured by ``requests``.  If ``PROXY_CA_FILE`` is set it
    takes precedence.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Dict, Mapping, MutableMapping, Optional
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from requests.sessions import Session
from requests.utils import get_environ_proxies

try:  # requests-kerberos is an optional dependency at runtime
    from requests_kerberos import HTTPKerberosAuth, OPTIONAL, REQUIRED, DISABLED
except Exception:  # pragma: no cover - guarded for environments without the lib
    HTTPKerberosAuth = None  # type: ignore
    OPTIONAL = REQUIRED = DISABLED = None  # type: ignore


LOG = logging.getLogger(__name__)


def _parse_headers(raw: str | None) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    if not raw:
        return headers
    for part in raw.split(";"):
        if not part:
            continue
        if ":" not in part:
            continue
        key, value = part.split(":", 1)
        headers[key.strip()] = value.strip()
    return headers


def _mutual_auth_mode(name: str | None):
    mapping = {
        "required": REQUIRED,
        "optional": OPTIONAL,
        "disabled": DISABLED,
    }
    if not name:
        return OPTIONAL
    key = name.strip().lower()
    return mapping.get(key, OPTIONAL)


@dataclass
class ProxyConfig:
    """Lightweight container for proxy configuration."""

    http: Optional[str]
    https: Optional[str]
    no_proxy: Optional[str]
    ca_file: Optional[str]
    headers: Dict[str, str]
    use_kerberos: bool
    kerberos_principal: Optional[str]
    kerberos_service: Optional[str]
    kerberos_mutual: Optional[str]

    @classmethod
    def from_env(cls) -> "ProxyConfig":
        env_proxies = get_environ_proxies("https://example.com")
        http_proxy = os.getenv("HTTP_PROXY", env_proxies.get("http"))
        https_proxy = os.getenv("HTTPS_PROXY", env_proxies.get("https"))
        no_proxy = os.getenv("NO_PROXY", env_proxies.get("no_proxy"))

        ca_file = (
            os.getenv("PROXY_CA_FILE")
            or os.getenv("REQUESTS_CA_BUNDLE")
            or os.getenv("CURL_CA_BUNDLE")
        )

        headers = _parse_headers(os.getenv("PROXY_HEADERS"))
        if not headers:
            # Default keep-alive header ensures long running downloads are stable
            headers = {"Proxy-Connection": "Keep-Alive"}

        use_kerberos = os.getenv("PROXY_KERBEROS", "true").lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

        return cls(
            http=http_proxy,
            https=https_proxy,
            no_proxy=no_proxy,
            ca_file=ca_file,
            headers=headers,
            use_kerberos=use_kerberos,
            kerberos_principal=os.getenv("PROXY_KERBEROS_PRINCIPAL"),
            kerberos_service=os.getenv("PROXY_KERBEROS_SERVICE"),
            kerberos_mutual=os.getenv("PROXY_KERBEROS_MUTUAL"),
        )

    def to_proxy_dict(self) -> Dict[str, str]:
        proxies: Dict[str, str] = {}
        if self.http:
            proxies["http"] = self.http
        if self.https:
            proxies["https"] = self.https
        if self.no_proxy:
            proxies["no_proxy"] = self.no_proxy
        return proxies


class HTTPAdapterWithProxyKerberosAuth(HTTPAdapter):
    """HTTP adapter that injects Kerberos ``Proxy-Authorization`` headers."""

    def __init__(
        self,
        *,
        proxy_headers: Optional[Mapping[str, str]] = None,
        ca_cert: Optional[str] = None,
        principal: Optional[str] = None,
        service: Optional[str] = None,
        mutual_authentication: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._proxy_headers = dict(proxy_headers or {})
        self._ca_cert = ca_cert
        if HTTPKerberosAuth is None:
            raise RuntimeError(
                "requests-kerberos is required to use HTTPAdapterWithProxyKerberosAuth"
            )
        self._kerberos = HTTPKerberosAuth(
            mutual_authentication=_mutual_auth_mode(mutual_authentication),
            principal=principal,
            service=service,
            force_preemptive=True,
        )

    def proxy_headers(self, proxy: str) -> MutableMapping[str, str]:  # type: ignore[override]
        headers = super().proxy_headers(proxy)
        headers.update(self._proxy_headers)
        return headers

    def cert_verify(self, conn, url, verify, cert):  # type: ignore[override]
        if verify is None and self._ca_cert:
            verify = self._ca_cert
        super().cert_verify(conn, url, verify, cert)

    def send(  # type: ignore[override]
        self,
        request,
        stream: bool = False,
        timeout: Optional[float] = None,
        verify=None,
        cert=None,
        proxies: Optional[Mapping[str, str]] = None,
    ):
        if proxies:
            proxy_url = proxies.get("https") or proxies.get("http")
            if proxy_url:
                host = urlparse(proxy_url).hostname
                if host:
                    try:
                        header = self._kerberos.generate_request_header(
                            request, host, is_preemptive=True
                        )
                    except Exception as exc:  # pragma: no cover - defensive
                        LOG.warning("Kerberos proxy header failed: %s", exc)
                    else:
                        if header:
                            request.headers["Proxy-Authorization"] = header
        if verify is None and self._ca_cert:
            verify = self._ca_cert
        return super().send(
            request,
            stream=stream,
            timeout=timeout,
            verify=verify,
            cert=cert,
            proxies=proxies,
        )


def create_session_from_env() -> Session:
    """Build a :class:`requests.Session` honouring proxy/Kerberos settings."""

    cfg = ProxyConfig.from_env()
    session = requests.Session()
    session.trust_env = False

    proxies = cfg.to_proxy_dict()
    if proxies:
        session.proxies.update(proxies)

    if cfg.ca_file:
        session.verify = cfg.ca_file

    adapter: HTTPAdapter
    if cfg.use_kerberos and (cfg.http or cfg.https):
        adapter = HTTPAdapterWithProxyKerberosAuth(
            proxy_headers=cfg.headers,
            ca_cert=cfg.ca_file,
            principal=cfg.kerberos_principal,
            service=cfg.kerberos_service,
            mutual_authentication=cfg.kerberos_mutual,
            max_retries=3,
        )
    else:
        adapter = HTTPAdapter(max_retries=3)

    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


__all__ = [
    "ProxyConfig",
    "create_session_from_env",
    "HTTPAdapterWithProxyKerberosAuth",
]

