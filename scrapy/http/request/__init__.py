"""
This module implements the Request class which is used to represent HTTP
requests in Scrapy.

See documentation in docs/topics/request-response.rst
"""
import inspect
from typing import Optional, Type, TypeVar

from w3lib.url import safe_url_string

import scrapy
from scrapy.http.common import obsolete_setter
from scrapy.http.headers import Headers
from scrapy.utils.curl import curl_to_request_kwargs
from scrapy.utils.misc import load_object
from scrapy.utils.python import to_bytes, to_unicode
from scrapy.utils.trackref import object_ref
from scrapy.utils.url import escape_ajax


RequestType = TypeVar("RequestType", bound="Request")
SpiderType = TypeVar("SpiderType", bound="scrapy.spiders.Spider")


class Request(object_ref):

    _attributes = (
        "url", "method", "headers", "body", "cookies", "meta", "flags",
        "encoding", "priority", "dont_filter", "callback", "errback", "cb_kwargs",
    )

    def __init__(self, url, callback=None, method='GET', headers=None, body=None,
                 cookies=None, meta=None, encoding='utf-8', priority=0,
                 dont_filter=False, errback=None, flags=None, cb_kwargs=None):

        self._encoding = encoding  # this one has to be set first
        self.method = str(method).upper()
        self._set_url(url)
        self._set_body(body)
        if not isinstance(priority, int):
            raise TypeError(f"Request priority not an integer: {priority!r}")
        self.priority = priority

        if callback is not None and not callable(callback):
            raise TypeError(f'callback must be a callable, got {type(callback).__name__}')
        if errback is not None and not callable(errback):
            raise TypeError(f'errback must be a callable, got {type(errback).__name__}')
        self.callback = callback
        self.errback = errback

        self.cookies = cookies or {}
        self.headers = Headers(headers or {}, encoding=encoding)
        self.dont_filter = dont_filter

        self._meta = dict(meta) if meta else None
        self._cb_kwargs = dict(cb_kwargs) if cb_kwargs else None
        self.flags = [] if flags is None else list(flags)

    @classmethod
    def from_dict(cls: Type[RequestType], d: dict, spider: Optional[SpiderType] = None) -> RequestType:
        """
        Create Request object from a dict.

        If a spider is given, it will try to resolve the callbacks looking at the
        spider for methods with the same name.
        """
        cb = d["callback"]
        if cb and spider:
            cb = _get_method(spider, cb)
        eb = d["errback"]
        if eb and spider:
            eb = _get_method(spider, eb)
        request_cls = load_object(d["_class"]) if "_class" in d else cls
        return request_cls(
            url=to_unicode(d["url"]),
            callback=cb,
            errback=eb,
            method=d["method"],
            headers=d["headers"],
            body=d["body"],
            cookies=d["cookies"],
            meta=d["meta"],
            encoding=d["_encoding"],
            priority=d["priority"],
            dont_filter=d["dont_filter"],
            flags=d.get("flags"),
            cb_kwargs=d.get("cb_kwargs"),
        )

    @property
    def cb_kwargs(self):
        if self._cb_kwargs is None:
            self._cb_kwargs = {}
        return self._cb_kwargs

    @property
    def meta(self):
        if self._meta is None:
            self._meta = {}
        return self._meta

    def _get_url(self):
        return self._url

    def _set_url(self, url):
        if not isinstance(url, str):
            raise TypeError(f'Request url must be str or unicode, got {type(url).__name__}')

        s = safe_url_string(url, self.encoding)
        self._url = escape_ajax(s)

        if (
            '://' not in self._url
            and not self._url.startswith('about:')
            and not self._url.startswith('data:')
        ):
            raise ValueError(f'Missing scheme in request url: {self._url}')

    url = property(_get_url, obsolete_setter(_set_url, 'url'))

    def _get_body(self):
        return self._body

    def _set_body(self, body):
        if body is None:
            self._body = b''
        else:
            self._body = to_bytes(body, self.encoding)

    body = property(_get_body, obsolete_setter(_set_body, 'body'))

    @property
    def encoding(self):
        return self._encoding

    def __str__(self):
        return f"<{self.method} {self.url}>"

    __repr__ = __str__

    def copy(self):
        """Return a copy of this Request"""
        return self.replace()

    def replace(self, *args, **kwargs):
        """Create a new Request with the same attributes except for those
        given new values.
        """
        for x in self._attributes:
            kwargs.setdefault(x, getattr(self, x))
        cls = kwargs.pop('cls', self.__class__)
        return cls(*args, **kwargs)

    @classmethod
    def from_curl(cls, curl_command, ignore_unknown_options=True, **kwargs):
        """Create a Request object from a string containing a `cURL
        <https://curl.haxx.se/>`_ command. It populates the HTTP method, the
        URL, the headers, the cookies and the body. It accepts the same
        arguments as the :class:`Request` class, taking preference and
        overriding the values of the same arguments contained in the cURL
        command.

        Unrecognized options are ignored by default. To raise an error when
        finding unknown options call this method by passing
        ``ignore_unknown_options=False``.

        .. caution:: Using :meth:`from_curl` from :class:`~scrapy.http.Request`
                     subclasses, such as :class:`~scrapy.http.JSONRequest`, or
                     :class:`~scrapy.http.XmlRpcRequest`, as well as having
                     :ref:`downloader middlewares <topics-downloader-middleware>`
                     and
                     :ref:`spider middlewares <topics-spider-middleware>`
                     enabled, such as
                     :class:`~scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware`,
                     :class:`~scrapy.downloadermiddlewares.useragent.UserAgentMiddleware`,
                     or
                     :class:`~scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware`,
                     may modify the :class:`~scrapy.http.Request` object.

        To translate a cURL command into a Scrapy request,
        you may use `curl2scrapy <https://michael-shub.github.io/curl2scrapy/>`_.

       """
        request_kwargs = curl_to_request_kwargs(curl_command, ignore_unknown_options)
        request_kwargs.update(kwargs)
        return cls(**request_kwargs)

    def to_dict(self, spider: Optional[SpiderType] = None) -> dict:
        """
        Convert Request object to a dict.

        If a spider is given, it will try to find out the name of the spider method
        used in the callback and store that as the callback.
        """
        cb = self.callback
        if callable(cb):
            cb = _find_method(spider, cb)
        eb = self.errback
        if callable(eb):
            eb = _find_method(spider, eb)
        d = {
            "url": to_unicode(self.url),  # urls should be safe (safe_string_url)
            "callback": cb,
            "errback": eb,
            "headers": dict(self.headers),
            "_encoding": self._encoding,
        }
        for attr in self._attributes:
            if attr not in d:
                d[attr] = getattr(self, attr)
        if type(self) is not Request:
            d["_class"] = self.__module__ + '.' + self.__class__.__name__
        return d


def _find_method(obj, func):
    """
    Helper function for Request.to_dict
    """
    # Only instance methods contain ``__func__``
    if obj and hasattr(func, '__func__'):
        members = inspect.getmembers(obj, predicate=inspect.ismethod)
        for name, obj_func in members:
            # We need to use __func__ to access the original
            # function object because instance method objects
            # are generated each time attribute is retrieved from
            # instance.
            #
            # Reference: The standard type hierarchy
            # https://docs.python.org/3/reference/datamodel.html
            if obj_func.__func__ is func.__func__:
                return name
    raise ValueError(f"Function {func} is not an instance method in: {obj}")


def _get_method(obj, name):
    """
    Helper function for Request.from_dict
    """
    name = str(name)
    try:
        return getattr(obj, name)
    except AttributeError:
        raise ValueError(f"Method {name!r} not found in: {obj}")
