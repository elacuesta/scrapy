from twisted.internet.interfaces import IHostnameResolver, IResolutionReceiver
from zope.interface.declarations import implementer, provider

from scrapy.utils.datatypes import LocalCache


# TODO: cache misses
dnscache = LocalCache(10000)


@implementer(IHostnameResolver)
class CachingHostnameResolver(object):

    def __init__(self, resolver, cache_size, timeout):
        self.resolver = resolver
        self.timeout = timeout
        dnscache.limit = cache_size

    def resolveHostName(self, resolutionReceiver, hostName, portNumber=0,
                        addressTypes=None, transportSemantics='TCP'):

        @provider(IResolutionReceiver)
        class CachingResolutionReceiver(resolutionReceiver):
            def resolutionBegan(self, resolution):
                super(CachingResolutionReceiver, self).resolutionBegan(resolution)
                self.resolution = resolution

            def resolutionComplete(self):
                super(CachingResolutionReceiver, self).resolutionComplete()
                dnscache[hostName] = self.resolution

        try:
            result = dnscache[hostName]
        except KeyError:
            result = self.resolver.resolveHostName(
                CachingResolutionReceiver(), hostName, portNumber, addressTypes, transportSemantics
            )
        finally:
            return result
