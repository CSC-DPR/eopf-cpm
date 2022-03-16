# We need to use a mix of posixpath (normpath) and pathlib (partition) in the eo_path methods.
# As we work with strings we use posixpath (the unix path specific implementation of os.path) as much as possible.
import posixpath
from pathlib import PurePosixPath
from typing import Any, Optional, Union

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
        if len(target) == 1 and isinstance(target[0], etree._Element):
            if target[0].tag.endswith("posList"):
                values = target[0].text.split(" ")
                merged_values = ",".join(" ".join([n, i]) for i, n in zip(values, values[1:]))
                return f"POLYGON(({merged_values}))"
            return target[0].text
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
    from numpy import (
        float16,
        float32,
        float64,
        int16,
        int32,
        int64,
        ndarray,
        uint16,
        uint32,
        uint64,
    )

    # check dict
    if isinstance(obj, dict):
        return {k: conv(v) for k, v in obj.items()}

    # check if list or tuple or set
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(map(conv, obj))

    # check numpy
    if isinstance(obj, ndarray):
        return conv(obj.tolist())

    # check np int
    if isinstance(obj, (int64, int32, int16, uint64, uint32, uint16, int)):
        return int(obj)

    # check np float
    if isinstance(obj, (float64, float32, float16, float)):
        return float(obj)

    # check str
    if isinstance(obj, str):
        return str(obj)

    # if no conversion can be done
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
        attrs = loads(attrs)
    except (JSONDecodeError, TypeError):
        return attrs


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
