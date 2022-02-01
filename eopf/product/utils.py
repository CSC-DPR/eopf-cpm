import functools
import weakref
from typing import Any, Callable, Optional


def parse_path(key: str) -> tuple[str, Optional[str]]:
    subkey = None
    if "/" in key:
        key, _, subkey = key.partition("/")
    return key, subkey


def join_path(*subpath: str, sep: str = "/") -> str:
    return sep.join(subpath)


def split_path(path: str, sep: str = "/") -> list[str]:
    return path.split(sep)


def weak_cache(func: Callable[..., Any]) -> Callable[..., Any]:
    @functools.cache
    def _func(_self: Any, *args: Any, **kwargs: Any) -> Any:
        return func(_self, *args, **kwargs)

    @functools.wraps(func)
    def inner(self: Any, *args: Any, **kwargs: Any) -> Any:
        return _func(weakref.proxy(self), *args, **kwargs)

    return inner
