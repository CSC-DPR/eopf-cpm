from typing import Any, Optional, Union

from eopf.product import EOProduct
from eopf.product.store.abstract import EOProductStore


def init_product(
    product_name: str, *, store_or_path_url: Optional[Union[str, EOProductStore]] = None, **kwargs: Any
) -> EOProduct:
    """Convenience function to create an harmonized common data model

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
    """
    product = EOProduct(product_name, store_or_path_url=store_or_path_url, **kwargs)

    for group_name in product.MANDATORY_FIELD:
        product.add_group(group_name)
    return product
