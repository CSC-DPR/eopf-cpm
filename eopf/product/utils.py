import functools
import posixpath
import weakref
from pathlib import PurePosixPath
from typing import Any, Callable, Optional


def join_path(*subpath: str, sep: str = "/") -> str:
    """Join elements from specific separator

    Parameters
    ----------
    *subpath: str
    sep: str, optional
        separator

    Returns
    -------
    str
    """
    return sep.join(subpath)


def weak_cache(func: Callable[..., Any]) -> Callable[..., Any]:
    """weak ref version of lru_cache for method

    See Also
    --------
    functools.cache
    """

    @functools.cache
    def _func(_self: Any, *args: Any, **kwargs: Any) -> Any:
        return func(_self(), *args, **kwargs)

    @functools.wraps(func)
    def inner(self: Any, *args: Any, **kwargs: Any) -> Any:
        return _func(weakref.ref(self), *args, **kwargs)

    return inner


# We need to use a mix of posixpath (normpath) and pathlib (partition) in the eo_path methods.
# As we work with strings we use posixpath (the unix path specific implementation of os.path) as much as possible.


def norm_eo_path(eo_path: str) -> str:
    """Normalize an eo object path.
    Parameters
    ----------
    eo_path: str

    Returns
    -------
    str
    """
    if eo_path == "":
        raise KeyError("Invalid empty eo_path")
    eo_path = posixpath.normpath(eo_path)  # Do not use pathlib (does not remove ..)
    if eo_path.startswith("//"):  # text is a special path so it's not normalised by normpath
        eo_path = eo_path[1:]
    return eo_path


def join_eo_path(*subpaths: str) -> str:
    """Join eo object paths.

    Parameters
    ----------
    *subpaths: str

    Returns
    -------
    str
    """
    return norm_eo_path(posixpath.join(*subpaths))


def join_eo_path_optional(*subpaths: Optional[str]) -> str:
    """Join eo object paths.

    Parameters
    ----------
    *subpaths: str

    Returns
    -------
    str
    """
    valid_subpaths = [path for path in subpaths if path]
    if not valid_subpaths:
        return ""
    return join_eo_path(*valid_subpaths)


def partition_eo_path(eo_path: str) -> tuple[str, ...]:
    """Extract each elements of the eo_path

    Parameters
    ----------
    eo_path: str

    Returns
    -------
    tuple[str, ...]
    """
    return PurePosixPath(eo_path).parts


def upsplit_eo_path(eo_path: str) -> tuple[str, ...]:
    """Split the given path

    Parameters
    ----------
    eo_path: str

    Returns
    -------
    tuple[str, ...]
    """
    return posixpath.split(eo_path)


def downsplit_eo_path(eo_path: str) -> tuple[str, Optional[str]]:
    """Extract base path and sub path

    Parameters
    ----------
    eo_path: str

    Returns
    -------
    str
        folder name
    str or None
        sub path
    """
    folder_name = partition_eo_path(eo_path)[0]
    sub_path: Optional[str] = posixpath.relpath(eo_path, start=folder_name)
    if sub_path == ".":
        sub_path = None
    return folder_name, sub_path


def is_absolute_eo_path(eo_path: str) -> bool:
    """Check if the given path is absolute or not

    Parameters
    ----------
    eo_path: str

    Returns
    -------
    bool
    """
    eo_path = norm_eo_path(eo_path)
    first_level, _ = downsplit_eo_path(eo_path)
    return first_level in ["/", ".."]


def product_relative_path(eo_context: str, eo_path: str) -> str:
    """Return eo_path relative to the product (an absolute path without the leading /).

    Parameters
    ----------
    eo_context: str
        base path context
    eo_path: str

    Returns
    -------
    str
    """
    absolute_path = join_eo_path(eo_context, eo_path)
    first_level_relative_path = downsplit_eo_path(absolute_path)[1]
    if first_level_relative_path is None:
        return ""
    return first_level_relative_path
