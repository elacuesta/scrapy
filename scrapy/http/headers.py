from typing import Any, AnyStr, List, Optional, Sequence, Union

from w3lib.http import headers_dict_to_raw

from scrapy.utils.datatypes import CaseInsensitiveDict
from scrapy.utils.decorators import deprecated
from scrapy.utils.python import to_unicode


class Headers(CaseInsensitiveDict):
    """Case insensitive http headers dictionary"""

    def __init__(self, seq: Optional[Sequence] = None, encoding: str = "utf-8"):
        self.encoding = encoding
        super().__init__(seq)

    def normkey(self, key: Union[AnyStr, int]) -> bytes:
        """Normalize key to bytes"""
        return self._tobytes(key).title()

    def normvalue(self, value: Union[Union[AnyStr, int], List[Union[AnyStr, int]]]) -> List[bytes]:
        """Normalize values to bytes"""
        if value is None:
            value = []
        elif isinstance(value, (str, bytes)):
            value = [value]
        elif not hasattr(value, '__iter__'):
            value = [value]
        return [self._tobytes(x) for x in value]

    def _tobytes(self, x: Union[AnyStr, int]) -> bytes:
        if isinstance(x, bytes):
            return x
        elif isinstance(x, str):
            return x.encode(self.encoding)
        elif isinstance(x, int):
            return str(x).encode(self.encoding)
        else:
            raise TypeError(f'Unsupported value type: {type(x)}')

    def __getitem__(self, key: AnyStr) -> bytes:
        try:
            return super().__getitem__(key)[-1]
        except IndexError:
            return None

    def __eq__(self, other: Any) -> bool:
        if len(self) != len(other):
            return False
        if not isinstance(other, Headers):
            try:
                other = Headers(other, encoding=self.encoding)
            except Exception:
                return False
        for k1, k2 in zip(sorted(self.keys()), sorted(other.keys())):
            if k1 != k2:
                return False
            if self.getlist(k1) != other.getlist(k2):
                return False
        return True

    def getlist(self, key: AnyStr, def_val: Optional[List[Union[AnyStr, int]]] = None) -> List[Union[AnyStr, int]]:
        try:
            return super().__getitem__(key)
        except KeyError:
            if def_val is not None:
                return self.normvalue(def_val)
            return []

    def appendlist(self, key: AnyStr, value: List[Union[AnyStr, int]]) -> None:
        lst = self.getlist(key)
        lst.extend(self.normvalue(value))
        self[key] = lst

    def to_unicode_dict(self) -> CaseInsensitiveDict:
        """Return headers as a CaseInsensitiveDict with unicode keys
        and unicode values. Multiple values are joined with ','.
        """
        return CaseInsensitiveDict(
            (to_unicode(key, encoding=self.encoding), to_unicode(b','.join(self.getlist(key)), encoding=self.encoding))
            for key in self.keys()
        )

    @deprecated
    def setlist(self, key, list_):
        self[key] = list_

    @deprecated
    def setlistdefault(self, key, default_list=()):
        return self.setdefault(key, default_list)

    @deprecated
    def to_string(self):
        return headers_dict_to_raw(self)
