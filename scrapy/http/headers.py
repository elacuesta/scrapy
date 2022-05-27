import warnings
from typing import List, Union

from w3lib.http import headers_dict_to_raw

from scrapy.exceptions import ScrapyDeprecationWarning
from scrapy.utils.datatypes import CaseInsensitiveDict, CaselessDict
from scrapy.utils.python import to_unicode


class _HeadersMixin:
    """TODO: merge this class with HTTPHeaders once Headers is removed,
    these methods are here to avoid code duplication.
    """

    def __init__(self, seq=None, encoding="utf-8"):
        self.encoding = encoding
        super().__init__(seq)

    def _tobytes(self, value: Union[str, bytes, int, float]) -> bytes:
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode(self.encoding)
        elif isinstance(value, (int, float)):
            return str(value).encode(self.encoding)
        else:
            raise TypeError(f"Unsupported value type: {type(value)}")

    def _normkey(self, key: Union[str, bytes]):
        return self._tobytes(key.title())

    def _normvalue(self, value: Union[str, bytes, int, float]) -> List[bytes]:
        if value is None:
            value = []
        elif isinstance(value, (str, bytes)):
            value = [value]
        elif not hasattr(value, "__iter__"):
            value = [value]
        return [self._tobytes(x) for x in value]

    def appendlist(self, key: Union[str, bytes], value: Union[str, bytes, int, float]) -> None:
        temp = self[key]
        temp.extend(self._normvalue(value))
        self[key] = temp

    def to_bytes(self) -> bytes:
        return headers_dict_to_raw(self)

    def to_unicode_dict(self):
        """Return headers as a CaseInsensitiveDict with unicode keys and values.
        Multiple values are joined with ",".
        """
        return CaseInsensitiveDict(
            (
                to_unicode(key, encoding=self.encoding),
                to_unicode(b",".join(value), encoding=self.encoding)
            )
            for key, value in self.items()
        )


class HTTPHeaders(_HeadersMixin, CaseInsensitiveDict):
    pass


class Headers(_HeadersMixin, CaselessDict):
    """Case insensitive http headers dictionary"""

    def __new__(cls, *args, **kwargs):
        if issubclass(cls, Headers):
            warnings.warn(
                "scrapy.http.headers.Headers is deprecated,"
                " please use scrapy.http.headers.HTTPHeaders instead",
                category=ScrapyDeprecationWarning,
                stacklevel=2,
            )
        return super().__new__(cls, *args, **kwargs)

    def normkey(self, key):
        return self._normkey(key)

    def normvalue(self, value):
        return self._normvalue(value)

    def to_string(self):
        return self.to_bytes()

    def __getitem__(self, key):
        try:
            return super().__getitem__(key)[-1]
        except IndexError:
            return None

    def get(self, key, def_val=None):
        try:
            return super().get(key, def_val)[-1]
        except IndexError:
            return None

    def getlist(self, key, def_val=None):
        try:
            return super().__getitem__(key)
        except KeyError:
            if def_val is not None:
                return self.normvalue(def_val)
            return []

    def setlist(self, key, list_):
        self[key] = list_

    def setlistdefault(self, key, default_list=()):
        return self.setdefault(key, default_list)

    def appendlist(self, key, value):
        lst = self.getlist(key)
        lst.extend(self.normvalue(value))
        self[key] = lst

    def items(self):
        return ((k, self.getlist(k)) for k in self.keys())

    def values(self):
        return [self[k] for k in self.keys()]

    def __copy__(self):
        return self.__class__(self)
    copy = __copy__
