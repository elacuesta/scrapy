from ipaddress import IPv4Address, IPv6Address
from typing import List, NoReturn, Optional, Sequence, Type, TypeVar, Union

from twisted.internet import defer
from twisted.internet.base import ReactorBase, ThreadedResolver
from twisted.internet.interfaces import IHostResolution, IHostnameResolver, IResolutionReceiver, IResolverSimple
from zope.interface.declarations import implementer, provider

from scrapy.crawler import Crawler
from scrapy.utils.datatypes import LocalCache


# TODO: cache misses
dnscache = LocalCache(10000)


CachingThreadedResolverTV = TypeVar("CachingThreadedResolverTV", bound="CachingThreadedResolver")
CachingHostnameResolverTV = TypeVar("CachingHostnameResolverTV", bound="CachingHostnameResolver")


@implementer(IResolverSimple)
class CachingThreadedResolver(ThreadedResolver):
    """
    Default caching resolver. IPv4 only, supports setting a timeout value for DNS requests.
    """

    def __init__(self, reactor: ReactorBase, cache_size: int, timeout: int) -> None:
        super().__init__(reactor)
        dnscache.limit = cache_size
        self.timeout = timeout

    @classmethod
    def from_crawler(
        cls: Type[CachingThreadedResolverTV], crawler: Crawler, reactor: ReactorBase
    ) -> CachingThreadedResolverTV:
        if crawler.settings.getbool('DNSCACHE_ENABLED'):
            cache_size = crawler.settings.getint('DNSCACHE_SIZE')
        else:
            cache_size = 0
        return cls(reactor, cache_size, crawler.settings.getfloat('DNS_TIMEOUT'))

    def install_on_reactor(self) -> None:
        self.reactor.installResolver(self)

    def getHostByName(self, name: str, timeout: Optional[int] = None) -> defer.Deferred:
        if name in dnscache:
            return defer.succeed(dnscache[name])
        # in Twisted<=16.6, getHostByName() is always called with
        # a default timeout of 60s (actually passed as (1, 3, 11, 45) tuple),
        # so the input argument above is simply overridden
        # to enforce Scrapy's DNS_TIMEOUT setting's value
        d = super().getHostByName(name=name, timeout=(self.timeout,))
        if dnscache.limit:
            d.addCallback(self._cache_result, name)
        return d

    def _cache_result(self, result: str, name: str) -> str:
        dnscache[name] = result
        return result


@implementer(IHostResolution)
class HostResolution:
    def __init__(self, name: str) -> None:
        self.name = name

    def cancel(self) -> NoReturn:
        raise NotImplementedError()


@provider(IResolutionReceiver)
class _CachingResolutionReceiver:
    def __init__(self, resolutionReceiver: Type[IResolutionReceiver], hostName: str) -> None:
        self.resolutionReceiver = resolutionReceiver
        self.hostName = hostName
        self.addresses: List[Union[IPv4Address, IPv6Address]] = []

    def resolutionBegan(self, resolution: HostResolution) -> None:
        self.resolutionReceiver.resolutionBegan(resolution)
        self.resolution = resolution

    def addressResolved(self, address: Union[IPv4Address, IPv6Address]) -> None:
        self.resolutionReceiver.addressResolved(address)
        self.addresses.append(address)

    def resolutionComplete(self) -> None:
        self.resolutionReceiver.resolutionComplete()
        if self.addresses:
            dnscache[self.hostName] = self.addresses


@implementer(IHostnameResolver)
class CachingHostnameResolver:
    """
    Experimental caching resolver. Resolves IPv4 and IPv6 addresses,
    does not support setting a timeout value for DNS requests.
    """

    def __init__(self, reactor: ReactorBase, cache_size: int) -> None:
        self.reactor = reactor
        self.original_resolver = reactor.nameResolver
        dnscache.limit = cache_size

    @classmethod
    def from_crawler(
        cls: Type[CachingHostnameResolverTV], crawler: Crawler, reactor: ReactorBase
    ) -> CachingHostnameResolverTV:
        if crawler.settings.getbool('DNSCACHE_ENABLED'):
            cache_size = crawler.settings.getint('DNSCACHE_SIZE')
        else:
            cache_size = 0
        return cls(reactor, cache_size)

    def install_on_reactor(self) -> None:
        self.reactor.installNameResolver(self)

    def resolveHostName(
        self,
        resolutionReceiver: Type[IResolutionReceiver],
        hostName: str,
        portNumber: int = 0,
        addressTypes: Optional[Sequence] = None,
        transportSemantics: str = "TCP",
    ) -> Type[IResolutionReceiver]:
        try:
            addresses = dnscache[hostName]
        except KeyError:
            return self.original_resolver.resolveHostName(
                _CachingResolutionReceiver(resolutionReceiver, hostName),
                hostName,
                portNumber,
                addressTypes,
                transportSemantics,
            )
        else:
            resolutionReceiver.resolutionBegan(HostResolution(hostName))
            for addr in addresses:
                resolutionReceiver.addressResolved(addr)
            resolutionReceiver.resolutionComplete()
            return resolutionReceiver
