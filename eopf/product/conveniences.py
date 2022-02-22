import glob
import os
from collections import defaultdict
from typing import Any, Dict, Hashable, Iterable, MutableMapping, Optional, Tuple, Union

import xarray as xr
from lxml import etree

from eopf.product.core import EOProduct
from eopf.product.store.abstract import EOProductStore


def init_product(
    product_name: str, *, store_or_path_url: Optional[Union[str, EOProductStore]] = None, **kwargs: Any
) -> EOProduct:
    """Convenience function to create a valid EOProduct base.

    Parameters
    ----------
    product_name: str
        name of the product to create
    store_or_path_url: Union[str, EOProductStore], optional
        a EOProductStore or a string to create to a EOZarrStore

    **kwargs: any
        Any valid named arguments for EOProduct

    See Also
    --------
    eopf.product.EOProduct
    eopf.product.EOProduct.is_valid
    """
    product = EOProduct(product_name, store_or_path_url=store_or_path_url, **kwargs)

    for group_name in product.MANDATORY_FIELD:
        product.add_group(group_name)
    return product


def filter_paths_by(paths: list[str], filters: list[str]) -> list[str]:
    """Filter the paths basename in a list of paths by taking into account given filters.

    Parameters
    ----------
    file_paths : list[str]
        Directories in which the search is performed.
    filters : list[str]
        Tokens to be contained in the filename.

    Returns
    -------
    filtered files : list[str]
    """
    filtered_file_paths: list[str] = []

    try:
        for path in paths:
            if path[-1] == os.path.sep:
                file_name = os.path.basename(path[:-1])
            else:
                file_name = os.path.basename(path)
            for filter in filters:
                if filter in file_name:
                    filtered_file_paths.append(path)
                    break
    except Exception as e:
        print(f"Exception encountered while filtering files: {e} ")
        return []

    return filtered_file_paths


def get_dir_files(dir_path: str, glob_pattern: str = "*") -> list[str]:
    """Find the files from a given directory with respect to a pattern.

    Parameters
    ----------
    dir_path : str
        Path to the directory.

    glob_pattern : str, optional:
        Pattern to be respected by the filenames.
        Defaults to "*"

    Returns:
    -------
    list of files: list[str]
    """
    dir_files: list[str] = []

    abs_dir_path = os.path.abspath(dir_path)
    if os.path.isdir(abs_dir_path):
        glob_search = os.path.join(abs_dir_path, glob_pattern)
        paths_in_dir = glob.glob(glob_search)
        for path in paths_in_dir:
            if os.path.isfile(path):
                dir_files.append(path)

    return dir_files


def read_xrd(
    files: list[str],
    skip: list[str] = [],
    pick: list[str] = [],
) -> Union[xr.Dataset, None]:
    """Read multiple xarray.Dataset from files.
    Optionally, filter the variables based on name.

    Parameters
    ----------
    files : list[str]:
        Paths of the files to be read from.
    skip : list[str], optional
        Variables which should not be read from . Defaults to [].
    pick : list[str], optional
        Variables which should be read from. Defaults to [].
    decode_times : bool, optional
        If True, decode variables and coordinates with time units into timedelta objects.
        If False, leave them encoded as numbers.
        Defaults to False.
    mask_and_scale : bool, optional
         If True, replace array values equal to _FillValue with NA and scale values
         according to the formula original_values * scale_factor + add_offset,
         where _FillValue, scale_factor and add_offset are taken from variable attributes (if they exist).
         Defaults to False.

    See Also
    --------
    xarray.open_dataset

    Returns
    -------
    dataset with the selected variables : xr.Dataset
    """
    variables: MutableMapping[Any, Any] = {}

    try:
        for file in files:
            file_ds = xr.open_dataset(file, decode_times=False, mask_and_scale=False)

            for k in file_ds.variables.keys():
                if k in skip:
                    continue
                if pick and (k not in pick):
                    continue
                variables[k] = file_ds.get(k)

        if variables == {}:
            return None

        return xr.Dataset(data_vars=variables)

    except Exception as e:
        print(f"Exception encountered: {e}")
        return None


def xrd_to_eovs(xrd: xr.Dataset) -> Iterable[Tuple[Hashable, xr.Variable]]:
    """Convert the variables contained in xarray.Dataset into EOVariable objects.

    Parameters
    ----------
    xrd : xr.Dataset
        Dataset which contains the variables to be converted.

    Returns
    -------
    dictionary with key corresponding to the name of the variable and
        value corresponding to the data contained : ItemsView[Hashable, Any]

    Yields
    ------
    Iterator[ItemsView[Hashable, Any]]
    """
    for xrd_var, xrd_val in sorted(xrd.variables.items()):
        yield xrd_var, xrd_val


def parse_xml(path: Any) -> Any:
    """Parse an XML file taking into account the pattern of the filename

    Parameters
    ----------
    path_to_product : str
        Path to the directory of the product
    glob_pattern : str
        Token identifying the file of interest

    Returns
    -------
    ElementTree object loaded with source elements : Any
    """
    dom = etree.parse(path)
    return dom


def parse_xml_string(content: Any) -> etree._ElementTree:
    """Parse and XML string

    Parameters
    ----------
    content : Any
        The content to be parsed

    Returns
    -------
    ElementTree object loaded with source elements : etree._ElementTree
    """
    dom = etree.fromstring(content)
    return etree._ElementTree(dom)


def translate_structure(
    map_value: Union[Dict[Any, Any], str],
    dom: Any,
    namespaces: Dict[str, str],
) -> Union[Dict[Any, Any], Any]:
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
    Translated structure : str
    """
    if isinstance(map_value, dict):
        return {attr: translate_structure(map_value[attr], dom, namespaces) for attr in map_value}
    elif isinstance(map_value, str):
        return apply_xpath(dom, map_value, namespaces)


def apply_xpath(dom: Any, xpath: str, namespaces: Dict[str, str]) -> str:
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
    The result of the XPath : str
    """
    target = dom.xpath(xpath, namespaces=namespaces)
    if isinstance(target, list):
        if len(target) == 1 and isinstance(target[0], etree._Element):
            if target[0].tag.endswith("posList"):
                values = target[0].text.split(" ")
                accu = "POLYGON((" + values[1] + " " + values[0]
                for i in range(2, len(values), 2):
                    accu += "," + values[i + 1] + " " + values[i]
                accu += "))"
                return accu
            else:
                return target[0].text
        else:
            return ",".join(target)
    return target


def etree_to_dict(t: etree._Element) -> dict[Any, Any]:
    """Convert the ElementTree into a dictionaty

    Parameters
    ----------
    t : Any
        Corresponds to the root element

    Returns
    -------
    Corresponding dictionary to the tree : dict
    """
    d: dict[Any, Any] = {t.tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.items():
                dd[k].append(v)
        d = {t.tag: {k: v[0] if len(v) == 1 else v for k, v in dd.items()}}
    if t.attrib:
        d[t.tag].update(("@" + k, v) for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
                d[t.tag]["#text"] = text
        else:
            d[t.tag] = text
    return d
