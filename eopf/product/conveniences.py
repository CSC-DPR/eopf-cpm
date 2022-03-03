from typing import Any, Dict, Optional, Union

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
                merged_values = ",".join(" ".join([n, i]) for i, n in zip(values, values[1:]))
                return f"POLYGON(({merged_values}))"
            return target[0].text
        return ",".join(target)
    return target
