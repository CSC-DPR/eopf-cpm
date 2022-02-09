import functools
import weakref
from os import path
from pathlib import PurePosixPath
from typing import Any, Callable, Optional, Tuple


def parse_path(key: str) -> tuple[str, Optional[str]]:
    subkey = None
    if "/" in key:
        key, _, subkey = key.partition("/")
    return key, subkey


def join_path(*subpath: str, sep: str = "/") -> str:
    return sep.join(subpath)


def split_path(path_str: str, sep: str = "/") -> list[str]:
    return path_str.split(sep)


def weak_cache(func: Callable[..., Any]) -> Callable[..., Any]:
    @functools.cache
    def _func(_self: Any, *args: Any, **kwargs: Any) -> Any:
        return func(_self(), *args, **kwargs)

    @functools.wraps(func)
    def inner(self: Any, *args: Any, **kwargs: Any) -> Any:
        return _func(weakref.ref(self), *args, **kwargs)

    return inner


# We need to use a mix of os.path (normpath) and pathlib (partition) in the eo_path methods.
# As we work with strings we use os.path as much as possible.


def norm_eo_path(eo_path: str) -> str:
    """
    Normalize an eo object path.
    Parameters
    ----------
    eo_path

    Returns
    -------

    """
    if eo_path == "":
        raise KeyError("Invalid empty eo_path")
    eo_path = path.normpath(eo_path)  # Do not use pathlib (does not remove ..)
    if eo_path.startswith("//"):  # text is a special path so it's not normalised by normpath
        eo_path = eo_path[1:]
    return eo_path


def join_eo_path(*subpaths: str) -> str:
    """
    Join eo object paths.
    Parameters
    ----------
    subpaths sub eo path to join

    Returns
    -------
    eo path string
    """
    return norm_eo_path(path.join(*subpaths))


def partition_eo_path(eo_path: str) -> Tuple[str, ...]:
    return PurePosixPath(eo_path).parts


def upsplit_eo_path(eo_path: str) -> Tuple[str, str]:
    return path.split(eo_path)


def downsplit_eo_path(eo_path: str) -> Tuple[str, Optional[str]]:
    folder_name = partition_eo_path(eo_path)[0]
    sub_path: Optional[str] = path.relpath(eo_path, start=folder_name)
    if sub_path == ".":
        sub_path = None
    return folder_name, sub_path


def is_absolute_eo_path(eo_path: str) -> bool:
    """

    Parameters
    ----------
    eo_path

    Returns
    -------

    """
    eo_path = norm_eo_path(eo_path)
    first_level, _ = downsplit_eo_path(eo_path)
    return first_level in ["/", ".."]


def product_relative_path(eo_context: str, eo_path: str) -> str:
    """
    Return eo_path relative to the product (an absolute path without the leading /).
    Parameters
    ----------
    eo_context
    eo_path

    Returns
    -------

    """
    absolute_path = join_eo_path(eo_context, eo_path)
    first_level_relative_path = downsplit_eo_path(absolute_path)[1]
    if first_level_relative_path is None:
        return ""
    return first_level_relative_path
