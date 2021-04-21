import base64
from urllib.parse import unquote, urlunparse
from urllib.request import getproxies, proxy_bypass, _parse_proxy  # type: ignore[attr-defined]
from typing import Optional, Tuple, Type, TypeVar

from scrapy.crawler import Crawler
from scrapy.exceptions import NotConfigured
from scrapy.http.request import Request
from scrapy.spiders import Spider
from scrapy.utils.httpobj import urlparse_cached
from scrapy.utils.python import to_bytes


HttpProxyMiddlewareTV = TypeVar("HttpProxyMiddlewareTV", bound="HttpProxyMiddleware")


class HttpProxyMiddleware:

    def __init__(self, auth_encoding: str = "latin-1") -> None:
        self.auth_encoding = auth_encoding
        self.proxies = {}
        for type_, url in getproxies().items():
            try:
                self.proxies[type_] = self._get_proxy(url, type_)
            # some values such as '/var/run/docker.sock' can't be parsed
            # by _parse_proxy and as such should be skipped
            except ValueError:
                continue

    @classmethod
    def from_crawler(cls: Type[HttpProxyMiddlewareTV], crawler: Crawler) -> HttpProxyMiddlewareTV:
        if not crawler.settings.getbool('HTTPPROXY_ENABLED'):
            raise NotConfigured
        auth_encoding = crawler.settings.get('HTTPPROXY_AUTH_ENCODING')
        return cls(auth_encoding)

    def process_request(self, request: Request, spider: Spider) -> None:
        # ignore if proxy is already set
        if 'proxy' in request.meta:
            if request.meta['proxy'] is None:
                return None
            # extract credentials if present
            creds, proxy_url = self._get_proxy(request.meta['proxy'], '')
            request.meta['proxy'] = proxy_url
            if creds and not request.headers.get('Proxy-Authorization'):
                request.headers['Proxy-Authorization'] = b'Basic ' + creds
            return None
        elif not self.proxies:
            return None

        parsed = urlparse_cached(request)
        scheme = parsed.scheme

        # 'no_proxy' is only supported by http schemes
        if scheme in ('http', 'https') and parsed.hostname and proxy_bypass(parsed.hostname):
            return None

        if scheme in self.proxies:
            self._set_proxy(request, scheme)

        return None

    def _basic_auth_header(self, username: str, password: str) -> bytes:
        return base64.b64encode(
            to_bytes(
                text=f'{unquote(username)}:{unquote(password)}',
                encoding=self.auth_encoding,
            )
        )

    def _get_proxy(self, url: str, orig_type: str) -> Tuple[Optional[bytes], str]:
        proxy_type, user, password, hostport = _parse_proxy(url)
        proxy_url = urlunparse((proxy_type or orig_type, hostport, '', '', '', ''))
        creds = self._basic_auth_header(user, password) if user else None
        return creds, proxy_url

    def _set_proxy(self, request: Request, scheme: str) -> None:
        creds, proxy = self.proxies[scheme]
        request.meta['proxy'] = proxy
        if creds:
            request.headers['Proxy-Authorization'] = b'Basic ' + creds
