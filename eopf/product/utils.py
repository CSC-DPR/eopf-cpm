# We need to use a mix of posixpath (normpath) and pathlib (partition) in the eo_path methods.
# As we work with strings we use posixpath (the unix path specific implementation of os.path) as much as possible.
import datetime
import platform
import posixpath
import re
from pathlib import PurePosixPath
from typing import Any, Callable, Optional, Union

import dask.array as da
import dateutil.parser as date_parser
import fsspec
import numpy as np
import pytz
import xarray
from lxml import etree


def apply_xpath(dom: Any, xpath: str, namespaces: dict[str, str]) -> str:
    """Apply the XPath on the DOM

    Parameters
    ----------
    dom : Any
        The DOM to be parsed with xpath
    xpath : str
        The value of the corresponding XPath rule
    namespaces : dict
        The associated namespaces

    Returns
    -------
    str
        The result of the XPath
    """
    target = dom.xpath(xpath, namespaces=namespaces)
    if isinstance(target, list):
        # Check if it's a list of Element and not text.
        if len(target) >= 1 and isinstance(target[0], etree._Element):
            if len(target) == 1:
                if target[0].tag.endswith("posList"):
                    values = target[0].text.split(" ")
                    merged_values = ",".join(" ".join([n, i]) for i, n in zip(values, values[1:]))
                    return f"POLYGON(({merged_values}))"
                return target[0].text
            target = [elt.text for elt in target]
        return ",".join(target)
    return target


def conv(obj: Any) -> Any:
    """Convert an object to native python data types if possible,
    otherwise return the the obj as it is

    Parameters
    ----------
    obj: Any
        an object

    Returns
    ----------
    Any
        either the obj conververted to native python data type,
        or the obj as received
    """

    # check dict
    if isinstance(obj, dict):
        return {k: conv(v) for k, v in obj.items()}

    # check if list or tuple or set
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(map(conv, obj))

    # check numpy
    if isinstance(obj, np.ndarray):
        return [conv(i) for i in obj.tolist()]

    # check np int
    if isinstance(obj, (*get_managed_numpy_dtype("int"), int)):
        return int(obj)

    # check np float
    if isinstance(obj, (*get_managed_numpy_dtype("float"), float)):
        return float(obj)

    # check str or datetime-like string
    if isinstance(obj, str):
        if is_date(obj):
            return convert_to_unix_time(obj)
        return str(obj)

    # check datetime
    if isinstance(obj, datetime.datetime):
        return convert_to_unix_time(obj)

    if isinstance(obj, bytes):
        return obj.decode()

    # if no conversion can be done
    return obj


def is_date(string: str) -> bool:
    """Return whether the string can be interpreted as a date.

    Parameters
    ----------
    string: str
        string to check for date

    Returns
    ----------
    bool
    """
    # minimal size of valid date is year size (4 character)
    if len(string) < 4:
        return False
    try:
        date_parser.parse(string)
        return True
    except ValueError:
        return False


def convert_to_unix_time(date: Any) -> Any:
    """Return whether the string can be interpreted as a date.

    Parameters
    ----------
    date: Any
        string or datetime to convert

    Returns
    ----------
    int
        unix time in microseconds
    """

    if isinstance(date, datetime.datetime):
        if date <= get_min_datetime_for_timestamp():
            return 0
        return int(date.timestamp() * 1000000)  # microseconds

    if isinstance(date, str):
        import pandas as pd

        start = pd.to_datetime(datetime.datetime.fromtimestamp(0, tz=pytz.UTC))
        try:
            end = pd.to_datetime(date)
            # Normalize data, if date is incomplete (missing timezone)
            if end.tzinfo is None:
                proxy_date = datetime.datetime(
                    end.year,
                    end.month,
                    end.day,
                    end.hour,
                    end.minute,
                    end.second,
                    0,
                    pytz.UTC,
                )
                end = pd.to_datetime(str(proxy_date))
        except pd.errors.OutOfBoundsDatetime:
            # Just return string if something went wrong.
            return str(date)
        time_delta = (end - start) // pd.Timedelta("1microsecond")
        return time_delta
    raise ValueError(f"{date} cannot be converted to an accepted format!")


def reverse_conv(data_type: Any, obj: Any) -> Any:
    """Converts the obj to the data_type

    Parameters
    ----------
    data_type: Any
        the data type to be converted to
    obj: Any
        an object

    Returns
    ----------
    Any
    """
    for dtype in get_managed_numpy_dtype():
        if np.dtype(data_type) == np.dtype(dtype):
            return dtype(obj)
    return obj


def decode_attrs(attrs: Any) -> Any:
    """Try to decode attributes as json if possible,
    otherwise return the attrs as they are

    Parameters
    ----------
    attrs: Any
        an object containing attributes

    Returns
    ----------
    Any
        either the attributes as json or the attributes as received
    """
    from json import JSONDecodeError, loads

    try:
        new_attrs = loads(attrs)
    except (JSONDecodeError, TypeError):
        return attrs
    return new_attrs


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


def fs_match_path(pattern: str, filesystem: fsspec.FSMap) -> str:
    """Find and return the first occurence of a path matchin
    a given pattern.

    If there is no match, pattern is return as it is.

    Parameters
    ----------
    pattern: str
        regex pattern to match
    filesystem    filesystem representation

    Returns
    -------
    str
        matching path if find, else `pattern`
    """
    filepath_regex = re.compile(pattern)
    for file_path in filesystem:
        if filepath_regex.fullmatch(file_path):
            return file_path
    return pattern


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


def get_min_datetime_for_timestamp():
    if platform.system() == "Windows":
        return (datetime.datetime.fromtimestamp(0, tz=pytz.UTC) + datetime.timedelta(days=1))
    return datetime.datetime.fromtimestamp(0, tz=pytz.UTC)


def get_managed_numpy_dtype(type_: Optional[str] = None) -> tuple[type, ...]:
    """Retrieve OS dependent dtype for numpy"""
    managed_type = {
        "uint": ("uint64", "uint32", "uint16", "uint8"),
    }
    managed_type["int"] = ("int64", "int32", "int16", "int8", *managed_type["uint"])
    managed_type["float"] = ("float64", "float32", "float16")
    if type_:
        types = managed_type.get(type_, tuple())
    else:
        types = [dtype for type_group in managed_type.values() for dtype in type_group]
    return tuple(getattr(np, attr) for attr in types if hasattr(np, attr))


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
    # Do not use pathlib (does not remove ..)
    eo_path = posixpath.normpath(eo_path)
    # text is a special path so it's not normalised by normpath
    if eo_path.startswith("//"):
        return eo_path[1:]
    return eo_path


def regex_path_append(path1: Optional[str], path2: Optional[str]) -> Optional[str]:
    """Append two (valid) regex path.
    Can use os/eo path append as regex path syntax is different.
    """
    if path1 is not None:
        path1 = path1.removesuffix("/")
    if path2 is None:
        return path1
    path2 = path2.removeprefix("/")
    if path1 is None:
        return path2
    return f"{path1}/{path2}"


def parse_xml(path: Any) -> Any:
    """Parse an XML file and create an object

    Parameters
    ----------
    path: str
        Path to the directory of the product
    Returns
    -------
    Any
        ElementTree object loaded with source elements :
    """
    return etree.parse(path)


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


def translate_structure(
    map_value: Union[dict[Any, Any], str],
    dom: Any,
    namespaces: dict[str, str],
) -> Union[dict[Any, Any], Any]:
    """Translate the structure into the appropiate data format.

    If the map is a dictionary, calls recursively the function in order to
    assign the values for each key.
    If the map is a string, calls `apply_xpath` function to determine the value.

    Parameters
    ----------
    map_value : Union[dict, str]
        Corresponds either to the xpath value, or to the dictionary of attributes
    dom : Any
        The DOM to be parsed with xpath
    namespaces : dict
        The associated namespaces
    Returns
    -------
    str
        Translated structure
    """
    if isinstance(map_value, dict):
        return {attr: translate_structure(map_value[attr], dom, namespaces) for attr in map_value}
    elif isinstance(map_value, str):
        return apply_xpath(dom, map_value, namespaces)


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


def xarray_to_data_map_block(
    func: Callable[[Any], Any], data_array: xarray.DataArray, *args: Any, **kwargs: Any
) -> da.Array:
    array = data_array.data
    if isinstance(array, da.Array):
        return da.map_blocks(func, array, *args, **kwargs)
    return func(array, *args, **kwargs)
